package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GCPDataSource is a specification for a GCPDataSource resource.
// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type GCPDataSource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec GCPDataSourceSpec `json:"spec"`
}

// GCPDataSourceSpec is the spec for a GCPDataSource resource.
type GCPDataSourceSpec struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	CloudStorage CloudStorage `json:"cloudStorage"`
}

// CloudStorage is the property for GCS data source information.
type CloudStorage struct {
	ServiceAccountName string `json:"serviceAccountName"`
	URI                string `json:"uri"`
}

// GCPDataSourceList is a list of GCPDataSource resources.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type GCPDataSourceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []GCPDataSource `json:"items"`
}
