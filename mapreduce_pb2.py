# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# NO CHECKED-IN PROTOBUF GENCODE
# source: mapreduce.proto
# Protobuf Python Version: 5.29.0
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(
    _runtime_version.Domain.PUBLIC,
    5,
    29,
    0,
    '',
    'mapreduce.proto'
)
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0fmapreduce.proto\x12\tmapreduce\"2\n\nJobRequest\x12\x10\n\x08job_name\x18\x01 \x01(\t\x12\x12\n\ninput_file\x18\x02 \x01(\t\"?\n\x0bJobResponse\x12\x0f\n\x07message\x18\x01 \x01(\t\x12\x0f\n\x07success\x18\x02 \x01(\x08\x12\x0e\n\x06job_id\x18\x03 \x01(\t2G\n\tMapReduce\x12:\n\tSubmitJob\x12\x15.mapreduce.JobRequest\x1a\x16.mapreduce.JobResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mapreduce_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_JOBREQUEST']._serialized_start=30
  _globals['_JOBREQUEST']._serialized_end=80
  _globals['_JOBRESPONSE']._serialized_start=82
  _globals['_JOBRESPONSE']._serialized_end=145
  _globals['_MAPREDUCE']._serialized_start=147
  _globals['_MAPREDUCE']._serialized_end=218
# @@protoc_insertion_point(module_scope)
