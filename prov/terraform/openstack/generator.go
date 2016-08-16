package openstack

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/imdario/mergo"
	"io/ioutil"
	"novaforge.bull.com/starlings-janus/janus/deployments"
	"novaforge.bull.com/starlings-janus/janus/log"
	"novaforge.bull.com/starlings-janus/janus/prov/terraform/commons"
	"os"
	"path"
	"path/filepath"
)

type Generator struct {
	kv *api.KV
}


func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil { return true, nil }
	if os.IsNotExist(err) { return false, nil }
	return true, err
}

func NewGenerator(kv *api.KV) *Generator {
	return &Generator{kv: kv}
}

func (g *Generator) getStringFormConsul(baseUrl, property string) (string, error) {
	getResult, _, err := g.kv.Get(baseUrl+"/"+property, nil)
	if err != nil {
		log.Printf("Can't get property %s for node %s", property, baseUrl)
		return "", fmt.Errorf("Can't get property %s for node %s: %v", property, baseUrl, err)
	}
	if getResult == nil {
		log.Debugf("Can't get property %s for node %s (not found)", property, baseUrl)
		return "", nil
	}
	return string(getResult.Value), nil
}

func addResource(infrastructure *commons.Infrastructure, resourceType, resourceName string, resource interface{}) {
	if len(infrastructure.Resource) != 0 {
		if infrastructure.Resource[resourceType] != nil && len(infrastructure.Resource[resourceType].(map[string]interface{})) != 0 {
			resourcesMap := infrastructure.Resource[resourceType].(map[string]interface{})
			resourcesMap[resourceName] = resource
		} else {
			resourcesMap := make(map[string]interface{})
			resourcesMap[resourceName] = resource
			infrastructure.Resource[resourceType] = resourcesMap
		}

	} else {
		resourcesMap := make(map[string]interface{})
		infrastructure.Resource = resourcesMap
		resourcesMap = make(map[string]interface{})
		resourcesMap[resourceName] = resource
		infrastructure.Resource[resourceType] = resourcesMap
	}
}

func (g *Generator) GenerateTerraformInfraForNode(depId, nodeName string) error {
	log.Debugf("Generating infrastructure for deployment with id %s", depId)
	nodeKey := path.Join(deployments.DeploymentKVPrefix, depId, "topology", "nodes", nodeName)
	var infrastructure commons.Infrastructure
	log.Debugf("inspecting node %s", nodeKey)
	kvPair, _, err := g.kv.Get(nodeKey+"/type", nil)
	if err != nil {
		log.Print(err)
		return err
	}
	nodeType := string(kvPair.Value)
	switch nodeType {
	case "janus.nodes.openstack.Compute":
		compute, err := g.generateOSInstance(nodeKey, depId)
		if err != nil {
			return err
		}
		addResource(&infrastructure, "openstack_compute_instance_v2", compute.Name, &compute)

		consulKey := commons.ConsulKey{Name: compute.Name + "-ip_address-key", Path: nodeKey + "/capabilities/endpoint/attributes/ip_address", Value: fmt.Sprintf("${openstack_compute_instance_v2.%s.access_ip_v4}", compute.Name)}
		consulKeyAttrib := commons.ConsulKey{Name: compute.Name + "-attrib_ip_address-key", Path: nodeKey + "/attributes/ip_address", Value: fmt.Sprintf("${openstack_compute_instance_v2.%s.access_ip_v4}", compute.Name)}
		consulKeyFixedIP := commons.ConsulKey{Name: compute.Name + "-ip_fixed_address-key", Path: nodeKey + "/attributes/private_address", Value: fmt.Sprintf("${openstack_compute_instance_v2.%s.network.0.fixed_ip_v4}", compute.Name)}

		var consulKeys commons.ConsulKeys
		if compute.FloatingIp != "" {
			consulKeyFloatingIP := commons.ConsulKey{Name: compute.Name + "-ip_floating_address-key", Path: nodeKey + "/attributes/public_address", Value: fmt.Sprintf("${openstack_compute_instance_v2.%s.floating_ip}", compute.Name)}
			consulKeys = commons.ConsulKeys{Keys: []commons.ConsulKey{consulKey, consulKeyAttrib, consulKeyFixedIP, consulKeyFloatingIP}}

		} else {
			consulKeys = commons.ConsulKeys{Keys: []commons.ConsulKey{consulKey, consulKeyFixedIP}}
		}

		addResource(&infrastructure, "consul_keys", compute.Name, &consulKeys)

	case "janus.nodes.openstack.BlockStorage":
		if volumeId, err := g.getStringFormConsul(nodeKey, "properties/volume_id"); err != nil {
			return err
		} else if volumeId != "" {
			log.Debugf("Reusing existing volume with id %q for node %q", volumeId, nodeName)
			return nil
		}
		bsvol, err := g.generateOSBSVolume(nodeKey)
		if err != nil {
			return err
		}

		addResource(&infrastructure, "openstack_blockstorage_volume_v1", nodeName, &bsvol)
		consulKey := commons.ConsulKey{Name: nodeName + "-bsVolumeID", Path: nodeKey + "/properties/volume_id", Value: fmt.Sprintf("${openstack_blockstorage_volume_v1.%s.id}", nodeName)}
		//consulKey2 := commons.ConsulKey{Name: nodeName + "-bsVolumeAttach", Path: nodeKey + "/properties/volume_attach", Value: fmt.Sprintf("element(${openstack_blockstorage_volume_v1.%s.attachment},2)", nodeName)}
		consulKeys := commons.ConsulKeys{Keys: []commons.ConsulKey{consulKey}}
		addResource(&infrastructure, "consul_keys", nodeName, &consulKeys)

	case "janus.nodes.openstack.FloatingIP":
		floatingIPString, err, isIp := g.generateFloatingIP(nodeKey)
		if err != nil {
			return err
		}

		consulKey := commons.ConsulKey{}
		if !isIp {
			floatingIP := FloatingIP{Pool: floatingIPString}
			addResource(&infrastructure, "openstack_compute_floatingip_v2", nodeName, &floatingIP)
			consulKey = commons.ConsulKey{Name: nodeName + "-floating_ip_address-key", Path: nodeKey + "/capabilities/endpoint/attributes/floating_ip_address", Value: fmt.Sprintf("${openstack_compute_floatingip_v2.%s.address}", nodeName)}
		} else {
			consulKey = commons.ConsulKey{Name: nodeName + "-floating_ip_address-key", Path: nodeKey + "/capabilities/endpoint/attributes/floating_ip_address", Value: floatingIPString}
		}
		consulKeys := commons.ConsulKeys{Keys: []commons.ConsulKey{consulKey}}
		addResource(&infrastructure, "consul_keys", nodeName, &consulKeys)

	default:
		return fmt.Errorf("Unsupported node type '%s' for node '%s' in deployment '%s'", nodeType, nodeName, depId)
	}

	jsonInfra, err := json.MarshalIndent(infrastructure, "", "  ")
	infraPath := filepath.Join("work", "deployments", fmt.Sprint(depId), "infra")
	log.Debugf("Try to lock the mutex")
	log.Debugf("%v : Infra processing", nodeName)
	if bool,err := exists(infraPath); !bool  {
		if err = os.MkdirAll(infraPath, 0775); err != nil {
			log.Printf("%+v", err)
			return err
		}
	}

	if bool,err := exists(filepath.Join(infraPath, "infra.tf.json")); bool {
		if err != nil {
			return err
		}

		var infrastructureLocal commons.Infrastructure

		if text, err := ioutil.ReadFile(filepath.Join(infraPath, "infra.tf.json")); err == nil {
			log.Debugf("%v", text)
 			if err = json.Unmarshal(text,&infrastructureLocal); err != nil {
				log.Debugf("%v",err)
				return err
			}
		} else {
			return err
		}

		log.Debugf("Local Infra : %v", infrastructureLocal)

		for Key, Value := range infrastructure.Resource {
			Valu := Value.(map[string]interface{})
			if val, ok := infrastructureLocal.Resource[Key]; ok {
				val2 := val.(map[string]interface{})
				if err := mergo.Map(&val2,Valu); err != nil {
					panic(err)
				}

				infrastructureLocal.Resource[Key] = val2

			} else {
				infrastructureLocal.Resource[Key] = Value
				log.Debugf("2: %v", Value)
			}
		}

		jsonInfra, err = json.MarshalIndent(infrastructureLocal, "", "  ")
	}

	if err = ioutil.WriteFile(filepath.Join(infraPath, "infra.tf.json"), jsonInfra, 0664); err != nil {
		log.Print("Failed to write file")
		return err
	}


	log.Printf("Infrastructure generated for deployment with id %s", depId)
	log.Debugf("Unlock the mutex")

	return nil
}
