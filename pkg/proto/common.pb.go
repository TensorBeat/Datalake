// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.25.0-devel
// 	protoc        v3.13.0
// source: tensorbeat/common.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AddFile struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name     string            `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Uri      string            `protobuf:"bytes,2,opt,name=uri,proto3" json:"uri,omitempty"`
	MimeType string            `protobuf:"bytes,3,opt,name=mimeType,proto3" json:"mimeType,omitempty"`
	Tags     map[string]string `protobuf:"bytes,4,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *AddFile) Reset() {
	*x = AddFile{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tensorbeat_common_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AddFile) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AddFile) ProtoMessage() {}

func (x *AddFile) ProtoReflect() protoreflect.Message {
	mi := &file_tensorbeat_common_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AddFile.ProtoReflect.Descriptor instead.
func (*AddFile) Descriptor() ([]byte, []int) {
	return file_tensorbeat_common_proto_rawDescGZIP(), []int{0}
}

func (x *AddFile) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *AddFile) GetUri() string {
	if x != nil {
		return x.Uri
	}
	return ""
}

func (x *AddFile) GetMimeType() string {
	if x != nil {
		return x.MimeType
	}
	return ""
}

func (x *AddFile) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

type File struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id       string            `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Name     string            `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Uri      string            `protobuf:"bytes,3,opt,name=uri,proto3" json:"uri,omitempty"`
	MimeType string            `protobuf:"bytes,4,opt,name=mimeType,proto3" json:"mimeType,omitempty"`
	Tags     map[string]string `protobuf:"bytes,5,rep,name=tags,proto3" json:"tags,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *File) Reset() {
	*x = File{}
	if protoimpl.UnsafeEnabled {
		mi := &file_tensorbeat_common_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *File) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*File) ProtoMessage() {}

func (x *File) ProtoReflect() protoreflect.Message {
	mi := &file_tensorbeat_common_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use File.ProtoReflect.Descriptor instead.
func (*File) Descriptor() ([]byte, []int) {
	return file_tensorbeat_common_proto_rawDescGZIP(), []int{1}
}

func (x *File) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *File) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *File) GetUri() string {
	if x != nil {
		return x.Uri
	}
	return ""
}

func (x *File) GetMimeType() string {
	if x != nil {
		return x.MimeType
	}
	return ""
}

func (x *File) GetTags() map[string]string {
	if x != nil {
		return x.Tags
	}
	return nil
}

var File_tensorbeat_common_proto protoreflect.FileDescriptor

var file_tensorbeat_common_proto_rawDesc = []byte{
	0x0a, 0x17, 0x74, 0x65, 0x6e, 0x73, 0x6f, 0x72, 0x62, 0x65, 0x61, 0x74, 0x2f, 0x63, 0x6f, 0x6d,
	0x6d, 0x6f, 0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x11, 0x74, 0x65, 0x6e, 0x73, 0x6f,
	0x72, 0x62, 0x65, 0x61, 0x74, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x22, 0xbe, 0x01, 0x0a,
	0x07, 0x41, 0x64, 0x64, 0x46, 0x69, 0x6c, 0x65, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03,
	0x75, 0x72, 0x69, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x69, 0x12, 0x1a,
	0x0a, 0x08, 0x6d, 0x69, 0x6d, 0x65, 0x54, 0x79, 0x70, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x08, 0x6d, 0x69, 0x6d, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x38, 0x0a, 0x04, 0x74, 0x61,
	0x67, 0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x24, 0x2e, 0x74, 0x65, 0x6e, 0x73, 0x6f,
	0x72, 0x62, 0x65, 0x61, 0x74, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x41, 0x64, 0x64,
	0x46, 0x69, 0x6c, 0x65, 0x2e, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04,
	0x74, 0x61, 0x67, 0x73, 0x1a, 0x37, 0x0a, 0x09, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0xc8, 0x01,
	0x0a, 0x04, 0x46, 0x69, 0x6c, 0x65, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x10, 0x0a, 0x03, 0x75, 0x72,
	0x69, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x75, 0x72, 0x69, 0x12, 0x1a, 0x0a, 0x08,
	0x6d, 0x69, 0x6d, 0x65, 0x54, 0x79, 0x70, 0x65, 0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08,
	0x6d, 0x69, 0x6d, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x35, 0x0a, 0x04, 0x74, 0x61, 0x67, 0x73,
	0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x21, 0x2e, 0x74, 0x65, 0x6e, 0x73, 0x6f, 0x72, 0x62,
	0x65, 0x61, 0x74, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x46, 0x69, 0x6c, 0x65, 0x2e,
	0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04, 0x74, 0x61, 0x67, 0x73, 0x1a,
	0x37, 0x0a, 0x09, 0x54, 0x61, 0x67, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03,
	0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14,
	0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76,
	0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x42, 0x0f, 0x5a, 0x0d, 0x2e, 0x2f, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x3b, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_tensorbeat_common_proto_rawDescOnce sync.Once
	file_tensorbeat_common_proto_rawDescData = file_tensorbeat_common_proto_rawDesc
)

func file_tensorbeat_common_proto_rawDescGZIP() []byte {
	file_tensorbeat_common_proto_rawDescOnce.Do(func() {
		file_tensorbeat_common_proto_rawDescData = protoimpl.X.CompressGZIP(file_tensorbeat_common_proto_rawDescData)
	})
	return file_tensorbeat_common_proto_rawDescData
}

var file_tensorbeat_common_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_tensorbeat_common_proto_goTypes = []interface{}{
	(*AddFile)(nil), // 0: tensorbeat.common.AddFile
	(*File)(nil),    // 1: tensorbeat.common.File
	nil,             // 2: tensorbeat.common.AddFile.TagsEntry
	nil,             // 3: tensorbeat.common.File.TagsEntry
}
var file_tensorbeat_common_proto_depIdxs = []int32{
	2, // 0: tensorbeat.common.AddFile.tags:type_name -> tensorbeat.common.AddFile.TagsEntry
	3, // 1: tensorbeat.common.File.tags:type_name -> tensorbeat.common.File.TagsEntry
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_tensorbeat_common_proto_init() }
func file_tensorbeat_common_proto_init() {
	if File_tensorbeat_common_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_tensorbeat_common_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AddFile); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_tensorbeat_common_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*File); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_tensorbeat_common_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_tensorbeat_common_proto_goTypes,
		DependencyIndexes: file_tensorbeat_common_proto_depIdxs,
		MessageInfos:      file_tensorbeat_common_proto_msgTypes,
	}.Build()
	File_tensorbeat_common_proto = out.File
	file_tensorbeat_common_proto_rawDesc = nil
	file_tensorbeat_common_proto_goTypes = nil
	file_tensorbeat_common_proto_depIdxs = nil
}
