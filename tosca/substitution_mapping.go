// Copyright 2018 Bull S.A.S. Atos Technologies - Bull, Rue Jean Jaures, B.P.68, 78340, Les Clayes-sous-Bois, France.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tosca

import (
	"github.com/pkg/errors"
)

// SubstitutionMapping allows to create a node type out of a given topology
// template. This allows the consumption of complex systems using a simplified
// vision.
//
// See http://docs.oasis-open.org/tosca/TOSCA-Simple-Profile-YAML/v1.2/TOSCA-Simple-Profile-YAML-v1.2.html
// section 3.8.12 Substitution mapping
type SubstitutionMapping struct {
	NodeType     string                     `yaml:"node_type"`
	Properties   map[string]PropAttrMapping `yaml:"properties,omitempty"`
	Capabilities map[string]CapReqMapping   `yaml:"capabilities,omitempty"`
	Requirements map[string]CapReqMapping   `yaml:"requirements,omitempty"`
	Attributes   map[string]PropAttrMapping `yaml:"attributes,omitempty"`
	Interfaces   map[string]string          `yaml:"interfaces,omitempty"`
}

// PropAttrMapping defines a property or attribute mapping.
// It accepts several grammars.
//
// - Single-line grammar:
//   <property_name>: <property_value>
//   or
//   <property_name>: [ <input_name> ]
//   or
//   <property_name>: [ <node_template_name>, <node_template_property_name> ]
//   or
//   <property_name>: [ <node_template_name>, <node_template_capability_name> | <node_template_requirement_name>, <property_name> ]
//
// - Multi-line grammar:
//   <property_name>:
//     mapping: [ < input_name > ]
//   or
//   <property_name>:
//     mapping: [ <node_template_name>, <node_template_property_name> ]
//   or
//   <property_name>:
//     mapping: [ <node_template_name>, <node_template_capability_name> | <node_template_requirement_name>, <property_name> ]
//   or
//   <property_name>:
//     value: <property_value>
//
// See http://docs.oasis-open.org/tosca/TOSCA-Simple-Profile-YAML/v1.2/TOSCA-Simple-Profile-YAML-v1.2.html
// section 3.8.8 Property mapping
type PropAttrMapping struct {
	Mapping []string         `yaml:"mapping,omitempty,flow"`
	Value   *ValueAssignment `yaml:"value,omitempty"`
}

// UnmarshalYAML unmarshals a yaml into a PropAttrMapping
func (c *PropAttrMapping) UnmarshalYAML(unmarshal func(interface{}) error) error {

	// Multi-line grammar check.
	// Example:
	// my_property:
	//   mapping: [node1, property1]
	// or
	// my_property:
	//   value: 1
	var str struct {
		Mapping []string         `yaml:"mapping,omitempty,flow"`
		Value   *ValueAssignment `yaml:"value,omitempty"`
	}

	if err := unmarshal(&str); err == nil {
		mappingSize := len(str.Mapping)
		if mappingSize != 0 {
			c.Mapping = str.Mapping
			if mappingSize > 3 {
				return errors.Errorf("Mapping should between 1 and 3 elements: %v", c.Mapping)
			}
			return nil
		}
		if str.Value != nil {
			c.Value = str.Value
			return nil
		}
	}

	// Single-line grammar check.
	// Example of property mapping using this format:
	//   my_property: [node1, property1]
	// or
	//   my_property: true
	var mapping []string
	if err := unmarshal(&mapping); err == nil {
		if len(mapping) > 0 {
			c.Mapping = mapping
			return nil
		}
	}

	var valueAssignment ValueAssignment
	err := unmarshal(&valueAssignment)
	if err == nil {
		c.Value = &valueAssignment
	}

	return err
}

// CapReqMapping defines a capability mapping or a requirement mapping.
// It accepts two grammars.
//
// - Single-line grammar:
//   <capability_name>: [ <node_template_name>, <node_template_capability_name> ]
//
// - Multi-line grammar:
//   <capability_name>:
//      mapping: [ <node_template_name>, <node_template_capability_name> ]
//        properties:
//          <property_name>: <property_value>
//        attributes:
//          <attribute_name>: <attribute_value>
//
// See http://docs.oasis-open.org/tosca/TOSCA-Simple-Profile-YAML/v1.2/TOSCA-Simple-Profile-YAML-v1.2.html
// section 3.8.9 Capability mapping and 3.8.10 Requirement mapping
type CapReqMapping struct {
	Mapping    []string                    `yaml:"mapping"`
	Properties map[string]*ValueAssignment `yaml:"properties,omitempty"`
	Attributes map[string]*ValueAssignment `yaml:"attributes,omitempty"`
}

// UnmarshalYAML unmarshals a yaml into a CapReqMapping
func (c *CapReqMapping) UnmarshalYAML(unmarshal func(interface{}) error) error {

	// First case, single-line grammar.
	// Example of capability mapping using this format:
	// exported_capability: [node1, internal_capability]
	var mapping []string
	if err := unmarshal(&mapping); err == nil {
		c.Mapping = mapping
	} else {

		// Second case, multi-line grammar.
		// Example:
		// exported_capability:
		//   mapping: [node1, internal_capability]
		//   properties:
		//     property1: value1
		var str struct {
			Mapping    []string                    `yaml:"mapping"`
			Properties map[string]*ValueAssignment `yaml:"properties,omitempty"`
			Attributes map[string]*ValueAssignment `yaml:"attributes,omitempty"`
		}

		if err := unmarshal(&str); err == nil {
			c.Mapping = str.Mapping
			c.Properties = str.Properties
			c.Attributes = str.Attributes
		} else {
			return err
		}
	}

	if len(c.Mapping) != 2 {
		return errors.Errorf("Mapping should have 2 elements: %v", c.Mapping)
	}

	return nil
}
