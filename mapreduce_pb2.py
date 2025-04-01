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




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0fmapreduce.proto\x12\tmapreduce\"2\n\nJobRequest\x12\x10\n\x08job_name\x18\x01 \x01(\t\x12\x12\n\ninput_file\x18\x02 \x01(\t\"?\n\x0bJobResponse\x12\x0f\n\x07message\x18\x01 \x01(\t\x12\x0f\n\x07success\x18\x02 \x01(\x08\x12\x0e\n\x06job_id\x18\x03 \x01(\t\"\\\n\x10HeartbeatRequest\x12\x11\n\tworker_id\x18\x01 \x01(\t\x12\x0e\n\x06status\x18\x02 \x01(\t\x12\x14\n\x0c\x63urrent_task\x18\x03 \x01(\t\x12\x0f\n\x07\x61\x64\x64ress\x18\x04 \x01(\t\":\n\x11HeartbeatResponse\x12\x14\n\x0c\x61\x63knowledged\x18\x01 \x01(\x08\x12\x0f\n\x07message\x18\x02 \x01(\t\"G\n\x07MapTask\x12\x0f\n\x07task_id\x18\x01 \x01(\t\x12\x12\n\ndata_chunk\x18\x02 \x01(\t\x12\x17\n\x0foutput_location\x18\x03 \x01(\t\"d\n\nReduceTask\x12\x0f\n\x07task_id\x18\x01 \x01(\t\x12,\n\x0bmapped_data\x18\x02 \x03(\x0b\x32\x17.mapreduce.KeyValuePair\x12\x17\n\x0foutput_location\x18\x03 \x01(\t\"j\n\x0cTaskResponse\x12\x0f\n\x07success\x18\x01 \x01(\x08\x12\x0f\n\x07task_id\x18\x02 \x01(\t\x12\x0f\n\x07message\x18\x03 \x01(\t\x12\'\n\x06result\x18\x04 \x03(\x0b\x32\x17.mapreduce.KeyValuePair\"*\n\x0cKeyValuePair\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\x05\x32\x93\x01\n\tMapReduce\x12:\n\tSubmitJob\x12\x15.mapreduce.JobRequest\x1a\x16.mapreduce.JobResponse\x12J\n\rSendHeartbeat\x12\x1b.mapreduce.HeartbeatRequest\x1a\x1c.mapreduce.HeartbeatResponse2\x8a\x01\n\x06Worker\x12<\n\rAssignMapTask\x12\x12.mapreduce.MapTask\x1a\x17.mapreduce.TaskResponse\x12\x42\n\x10\x41ssignReduceTask\x12\x15.mapreduce.ReduceTask\x1a\x17.mapreduce.TaskResponseb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'mapreduce_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_JOBREQUEST']._serialized_start=30
  _globals['_JOBREQUEST']._serialized_end=80
  _globals['_JOBRESPONSE']._serialized_start=82
  _globals['_JOBRESPONSE']._serialized_end=145
  _globals['_HEARTBEATREQUEST']._serialized_start=147
  _globals['_HEARTBEATREQUEST']._serialized_end=239
  _globals['_HEARTBEATRESPONSE']._serialized_start=241
  _globals['_HEARTBEATRESPONSE']._serialized_end=299
  _globals['_MAPTASK']._serialized_start=301
  _globals['_MAPTASK']._serialized_end=372
  _globals['_REDUCETASK']._serialized_start=374
  _globals['_REDUCETASK']._serialized_end=474
  _globals['_TASKRESPONSE']._serialized_start=476
  _globals['_TASKRESPONSE']._serialized_end=582
  _globals['_KEYVALUEPAIR']._serialized_start=584
  _globals['_KEYVALUEPAIR']._serialized_end=626
  _globals['_MAPREDUCE']._serialized_start=629
  _globals['_MAPREDUCE']._serialized_end=776
  _globals['_WORKER']._serialized_start=779
  _globals['_WORKER']._serialized_end=917
# @@protoc_insertion_point(module_scope)
