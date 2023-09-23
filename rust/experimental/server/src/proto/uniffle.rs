#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinishShuffleRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FinishShuffleResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequireBufferRequest {
    #[prost(int32, tag = "1")]
    pub require_size: i32,
    #[prost(string, tag = "2")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "3")]
    pub shuffle_id: i32,
    #[prost(int32, repeated, tag = "4")]
    pub partition_ids: ::prost::alloc::vec::Vec<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequireBufferResponse {
    #[prost(int64, tag = "1")]
    pub require_buffer_id: i64,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status: i32,
    #[prost(string, tag = "3")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleDataBlockSegment {
    #[prost(int64, tag = "1")]
    pub block_id: i64,
    #[prost(int64, tag = "2")]
    pub offset: i64,
    #[prost(int32, tag = "3")]
    pub length: i32,
    #[prost(int32, tag = "4")]
    pub uncompress_length: i32,
    #[prost(int64, tag = "5")]
    pub crc: i64,
    #[prost(int64, tag = "6")]
    pub task_attempt_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLocalShuffleDataRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "3")]
    pub partition_id: i32,
    #[prost(int32, tag = "4")]
    pub partition_num_per_range: i32,
    #[prost(int32, tag = "5")]
    pub partition_num: i32,
    #[prost(int64, tag = "6")]
    pub offset: i64,
    #[prost(int32, tag = "7")]
    pub length: i32,
    #[prost(int64, tag = "8")]
    pub timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLocalShuffleDataResponse {
    #[prost(bytes = "bytes", tag = "1")]
    pub data: ::prost::bytes::Bytes,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status: i32,
    #[prost(string, tag = "3")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetMemoryShuffleDataRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "3")]
    pub partition_id: i32,
    #[prost(int64, tag = "4")]
    pub last_block_id: i64,
    #[prost(int32, tag = "5")]
    pub read_buffer_size: i32,
    #[prost(int64, tag = "6")]
    pub timestamp: i64,
    #[prost(bytes = "bytes", tag = "7")]
    pub serialized_expected_task_ids_bitmap: ::prost::bytes::Bytes,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetMemoryShuffleDataResponse {
    #[prost(message, repeated, tag = "1")]
    pub shuffle_data_block_segments: ::prost::alloc::vec::Vec<ShuffleDataBlockSegment>,
    #[prost(bytes = "bytes", tag = "2")]
    pub data: ::prost::bytes::Bytes,
    #[prost(enumeration = "StatusCode", tag = "3")]
    pub status: i32,
    #[prost(string, tag = "4")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLocalShuffleIndexRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "3")]
    pub partition_id: i32,
    #[prost(int32, tag = "4")]
    pub partition_num_per_range: i32,
    #[prost(int32, tag = "5")]
    pub partition_num: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetLocalShuffleIndexResponse {
    #[prost(bytes = "bytes", tag = "1")]
    pub index_data: ::prost::bytes::Bytes,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status: i32,
    #[prost(string, tag = "3")]
    pub ret_msg: ::prost::alloc::string::String,
    #[prost(int64, tag = "4")]
    pub data_file_len: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportShuffleResultRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int64, tag = "3")]
    pub task_attempt_id: i64,
    #[prost(int32, tag = "4")]
    pub bitmap_num: i32,
    #[prost(message, repeated, tag = "5")]
    pub partition_to_block_ids: ::prost::alloc::vec::Vec<PartitionToBlockIds>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionToBlockIds {
    #[prost(int32, tag = "1")]
    pub partition_id: i32,
    #[prost(int64, repeated, tag = "2")]
    pub block_ids: ::prost::alloc::vec::Vec<i64>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportShuffleResultResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleResultRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "3")]
    pub partition_id: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleResultResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
    #[prost(bytes = "bytes", tag = "3")]
    pub serialized_bitmap: ::prost::bytes::Bytes,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleResultForMultiPartRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, repeated, tag = "3")]
    pub partitions: ::prost::alloc::vec::Vec<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleResultForMultiPartResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
    #[prost(bytes = "bytes", tag = "3")]
    pub serialized_bitmap: ::prost::bytes::Bytes,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShufflePartitionRange {
    #[prost(int32, tag = "1")]
    pub start: i32,
    #[prost(int32, tag = "2")]
    pub end: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRegisterRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(message, repeated, tag = "3")]
    pub partition_ranges: ::prost::alloc::vec::Vec<ShufflePartitionRange>,
    #[prost(message, optional, tag = "4")]
    pub remote_storage: ::core::option::Option<RemoteStorage>,
    #[prost(string, tag = "5")]
    pub user: ::prost::alloc::string::String,
    #[prost(enumeration = "DataDistribution", tag = "6")]
    pub shuffle_data_distribution: i32,
    #[prost(int32, tag = "7")]
    pub max_concurrency_per_partition_to_write: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleUnregisterRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleUnregisterResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleRegisterResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SendShuffleDataRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int64, tag = "3")]
    pub require_buffer_id: i64,
    #[prost(message, repeated, tag = "4")]
    pub shuffle_data: ::prost::alloc::vec::Vec<ShuffleData>,
    #[prost(int64, tag = "5")]
    pub timestamp: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SendShuffleDataResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleData {
    #[prost(int32, tag = "1")]
    pub partition_id: i32,
    #[prost(message, repeated, tag = "2")]
    pub block: ::prost::alloc::vec::Vec<ShuffleBlock>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleBlock {
    #[prost(int64, tag = "1")]
    pub block_id: i64,
    #[prost(int32, tag = "2")]
    pub length: i32,
    #[prost(int32, tag = "3")]
    pub uncompress_length: i32,
    #[prost(int64, tag = "4")]
    pub crc: i64,
    #[prost(bytes = "bytes", tag = "5")]
    pub data: ::prost::bytes::Bytes,
    #[prost(int64, tag = "6")]
    pub task_attempt_id: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleCommitRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleCommitResponse {
    #[prost(int32, tag = "1")]
    pub commit_count: i32,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status: i32,
    #[prost(string, tag = "3")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleServerHeartBeatRequest {
    #[prost(message, optional, tag = "1")]
    pub server_id: ::core::option::Option<ShuffleServerId>,
    #[prost(int64, tag = "2")]
    pub used_memory: i64,
    #[prost(int64, tag = "3")]
    pub pre_allocated_memory: i64,
    #[prost(int64, tag = "4")]
    pub available_memory: i64,
    #[prost(int32, tag = "5")]
    pub event_num_in_flush: i32,
    #[prost(string, repeated, tag = "6")]
    pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "7")]
    pub is_healthy: ::core::option::Option<bool>,
    #[prost(enumeration = "ServerStatus", tag = "8")]
    pub status: i32,
    /// mount point to storage info mapping.
    #[prost(map = "string, message", tag = "21")]
    pub storage_info: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        StorageInfo,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleServerHeartBeatResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleServerId {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub ip: ::prost::alloc::string::String,
    #[prost(int32, tag = "3")]
    pub port: i32,
    #[prost(int32, tag = "4")]
    pub netty_port: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ShuffleServerResult {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageInfo {
    #[prost(string, tag = "1")]
    pub mount_point: ::prost::alloc::string::String,
    #[prost(enumeration = "storage_info::StorageMedia", tag = "2")]
    pub storage_media: i32,
    #[prost(int64, tag = "3")]
    pub capacity: i64,
    #[prost(int64, tag = "4")]
    pub used_bytes: i64,
    /// writing speed of last minute
    #[prost(int64, tag = "5")]
    pub writing_speed1_m: i64,
    /// writing speed of last 5 minutes
    #[prost(int64, tag = "6")]
    pub writing_speed5_m: i64,
    /// writing speed of last hour
    #[prost(int64, tag = "7")]
    pub writing_speed1_h: i64,
    /// number of writing failures since start up.
    #[prost(int64, tag = "8")]
    pub num_of_writing_failures: i64,
    #[prost(enumeration = "storage_info::StorageStatus", tag = "9")]
    pub status: i32,
}
/// Nested message and enum types in `StorageInfo`.
pub mod storage_info {
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum StorageMedia {
        StorageTypeUnknown = 0,
        Hdd = 1,
        Ssd = 2,
        Hdfs = 3,
        /// possible other types, such as cloud-ssd.
        ObjectStore = 4,
    }
    impl StorageMedia {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                StorageMedia::StorageTypeUnknown => "STORAGE_TYPE_UNKNOWN",
                StorageMedia::Hdd => "HDD",
                StorageMedia::Ssd => "SSD",
                StorageMedia::Hdfs => "HDFS",
                StorageMedia::ObjectStore => "OBJECT_STORE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "STORAGE_TYPE_UNKNOWN" => Some(Self::StorageTypeUnknown),
                "HDD" => Some(Self::Hdd),
                "SSD" => Some(Self::Ssd),
                "HDFS" => Some(Self::Hdfs),
                "OBJECT_STORE" => Some(Self::ObjectStore),
                _ => None,
            }
        }
    }
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum StorageStatus {
        Unknown = 0,
        Normal = 1,
        Unhealthy = 2,
        /// indicate current disk/storage is overused.
        Overused = 3,
    }
    impl StorageStatus {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                StorageStatus::Unknown => "STORAGE_STATUS_UNKNOWN",
                StorageStatus::Normal => "NORMAL",
                StorageStatus::Unhealthy => "UNHEALTHY",
                StorageStatus::Overused => "OVERUSED",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "STORAGE_STATUS_UNKNOWN" => Some(Self::Unknown),
                "NORMAL" => Some(Self::Normal),
                "UNHEALTHY" => Some(Self::Unhealthy),
                "OVERUSED" => Some(Self::Overused),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppHeartBeatRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AppHeartBeatResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplicationInfoRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub user: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplicationInfoResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleServerListResponse {
    #[prost(message, repeated, tag = "1")]
    pub servers: ::prost::alloc::vec::Vec<ShuffleServerId>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleServerNumResponse {
    #[prost(int32, tag = "1")]
    pub num: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleServerRequest {
    #[prost(string, tag = "1")]
    pub client_host: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub client_port: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub client_property: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub application_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "5")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "6")]
    pub partition_num: i32,
    #[prost(int32, tag = "7")]
    pub partition_num_per_range: i32,
    #[prost(int32, tag = "8")]
    pub data_replica: i32,
    #[prost(string, repeated, tag = "9")]
    pub require_tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(int32, tag = "10")]
    pub assignment_shuffle_server_number: i32,
    #[prost(int32, tag = "11")]
    pub estimate_task_concurrency: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionRangeAssignment {
    #[prost(int32, tag = "1")]
    pub start_partition: i32,
    #[prost(int32, tag = "2")]
    pub end_partition: i32,
    /// replica
    #[prost(message, repeated, tag = "3")]
    pub server: ::prost::alloc::vec::Vec<ShuffleServerId>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleAssignmentsResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(message, repeated, tag = "2")]
    pub assignments: ::prost::alloc::vec::Vec<PartitionRangeAssignment>,
    #[prost(string, tag = "3")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportShuffleClientOpRequest {
    #[prost(string, tag = "1")]
    pub client_host: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub client_port: i32,
    #[prost(message, optional, tag = "3")]
    pub server: ::core::option::Option<ShuffleServerId>,
    #[prost(string, tag = "4")]
    pub operation: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportShuffleClientOpResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetShuffleDataStorageInfoResponse {
    #[prost(string, tag = "1")]
    pub storage: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub storage_path: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub storage_pattern: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CheckServiceAvailableResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(bool, tag = "2")]
    pub available: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccessClusterRequest {
    #[prost(string, tag = "1")]
    pub access_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "2")]
    pub tags: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(map = "string, string", tag = "3")]
    pub extra_properties: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    #[prost(string, tag = "4")]
    pub user: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccessClusterResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub uuid: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchClientConfResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub client_conf: ::prost::alloc::vec::Vec<ClientConfItem>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ClientConfItem {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchRemoteStorageRequest {
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteStorageConfItem {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RemoteStorage {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub remote_storage_conf: ::prost::alloc::vec::Vec<RemoteStorageConfItem>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchRemoteStorageResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(message, optional, tag = "2")]
    pub remote_storage: ::core::option::Option<RemoteStorage>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecommissionRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecommissionResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelDecommissionRequest {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CancelDecommissionResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(string, tag = "2")]
    pub ret_msg: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportShuffleFetchFailureRequest {
    /// appId normally should be omitted, it's used to avoid wrongly request issued from remaining executors of another
    /// app which accidentally has the same shuffle manager port with this app.
    #[prost(string, tag = "1")]
    pub app_id: ::prost::alloc::string::String,
    #[prost(int32, tag = "2")]
    pub shuffle_id: i32,
    #[prost(int32, tag = "3")]
    pub stage_attempt_id: i32,
    #[prost(int32, tag = "4")]
    pub partition_id: i32,
    /// todo: report ShuffleServerId if needed
    /// ShuffleServerId serverId = 6;
    #[prost(string, tag = "5")]
    pub exception: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReportShuffleFetchFailureResponse {
    #[prost(enumeration = "StatusCode", tag = "1")]
    pub status: i32,
    #[prost(bool, tag = "2")]
    pub re_submit_whole_stage: bool,
    #[prost(string, tag = "3")]
    pub msg: ::prost::alloc::string::String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DataDistribution {
    Normal = 0,
    LocalOrder = 1,
}
impl DataDistribution {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DataDistribution::Normal => "NORMAL",
            DataDistribution::LocalOrder => "LOCAL_ORDER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NORMAL" => Some(Self::Normal),
            "LOCAL_ORDER" => Some(Self::LocalOrder),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ServerStatus {
    Active = 0,
    Decommissioning = 1,
    /// todo: more status, such as UPGRADING
    Decommissioned = 2,
}
impl ServerStatus {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ServerStatus::Active => "ACTIVE",
            ServerStatus::Decommissioning => "DECOMMISSIONING",
            ServerStatus::Decommissioned => "DECOMMISSIONED",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ACTIVE" => Some(Self::Active),
            "DECOMMISSIONING" => Some(Self::Decommissioning),
            "DECOMMISSIONED" => Some(Self::Decommissioned),
            _ => None,
        }
    }
}
/// * Status code to identify the status of response
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum StatusCode {
    Success = 0,
    DoubleRegister = 1,
    NoBuffer = 2,
    InvalidStorage = 3,
    NoRegister = 4,
    NoPartition = 5,
    InternalError = 6,
    Timeout = 7,
    AccessDenied = 8,
    /// add more status
    InvalidRequest = 9,
}
impl StatusCode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StatusCode::Success => "SUCCESS",
            StatusCode::DoubleRegister => "DOUBLE_REGISTER",
            StatusCode::NoBuffer => "NO_BUFFER",
            StatusCode::InvalidStorage => "INVALID_STORAGE",
            StatusCode::NoRegister => "NO_REGISTER",
            StatusCode::NoPartition => "NO_PARTITION",
            StatusCode::InternalError => "INTERNAL_ERROR",
            StatusCode::Timeout => "TIMEOUT",
            StatusCode::AccessDenied => "ACCESS_DENIED",
            StatusCode::InvalidRequest => "INVALID_REQUEST",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SUCCESS" => Some(Self::Success),
            "DOUBLE_REGISTER" => Some(Self::DoubleRegister),
            "NO_BUFFER" => Some(Self::NoBuffer),
            "INVALID_STORAGE" => Some(Self::InvalidStorage),
            "NO_REGISTER" => Some(Self::NoRegister),
            "NO_PARTITION" => Some(Self::NoPartition),
            "INTERNAL_ERROR" => Some(Self::InternalError),
            "TIMEOUT" => Some(Self::Timeout),
            "ACCESS_DENIED" => Some(Self::AccessDenied),
            "INVALID_REQUEST" => Some(Self::InvalidRequest),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod shuffle_server_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ShuffleServerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShuffleServerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ShuffleServerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ShuffleServerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ShuffleServerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn register_shuffle(
            &mut self,
            request: impl tonic::IntoRequest<super::ShuffleRegisterRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleRegisterResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/registerShuffle",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.ShuffleServer", "registerShuffle"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn unregister_shuffle(
            &mut self,
            request: impl tonic::IntoRequest<super::ShuffleUnregisterRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleUnregisterResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/unregisterShuffle",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServer", "unregisterShuffle"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn send_shuffle_data(
            &mut self,
            request: impl tonic::IntoRequest<super::SendShuffleDataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::SendShuffleDataResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/sendShuffleData",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.ShuffleServer", "sendShuffleData"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_local_shuffle_index(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLocalShuffleIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLocalShuffleIndexResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/getLocalShuffleIndex",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServer", "getLocalShuffleIndex"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_local_shuffle_data(
            &mut self,
            request: impl tonic::IntoRequest<super::GetLocalShuffleDataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLocalShuffleDataResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/getLocalShuffleData",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServer", "getLocalShuffleData"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_memory_shuffle_data(
            &mut self,
            request: impl tonic::IntoRequest<super::GetMemoryShuffleDataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetMemoryShuffleDataResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/getMemoryShuffleData",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServer", "getMemoryShuffleData"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn commit_shuffle_task(
            &mut self,
            request: impl tonic::IntoRequest<super::ShuffleCommitRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleCommitResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/commitShuffleTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServer", "commitShuffleTask"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn report_shuffle_result(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportShuffleResultRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportShuffleResultResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/reportShuffleResult",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServer", "reportShuffleResult"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_shuffle_result(
            &mut self,
            request: impl tonic::IntoRequest<super::GetShuffleResultRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleResultResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/getShuffleResult",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.ShuffleServer", "getShuffleResult"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn get_shuffle_result_for_multi_part(
            &mut self,
            request: impl tonic::IntoRequest<super::GetShuffleResultForMultiPartRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleResultForMultiPartResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/getShuffleResultForMultiPart",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.ShuffleServer",
                        "getShuffleResultForMultiPart",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn finish_shuffle(
            &mut self,
            request: impl tonic::IntoRequest<super::FinishShuffleRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FinishShuffleResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/finishShuffle",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.ShuffleServer", "finishShuffle"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn require_buffer(
            &mut self,
            request: impl tonic::IntoRequest<super::RequireBufferRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RequireBufferResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/requireBuffer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.ShuffleServer", "requireBuffer"));
            self.inner.unary(req, path, codec).await
        }
        pub async fn app_heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::AppHeartBeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AppHeartBeatResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServer/appHeartbeat",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.ShuffleServer", "appHeartbeat"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod coordinator_server_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct CoordinatorServerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl CoordinatorServerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> CoordinatorServerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> CoordinatorServerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            CoordinatorServerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// Get Shuffle Server list
        pub async fn get_shuffle_server_list(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleServerListResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/getShuffleServerList",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "getShuffleServerList",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Count Shuffle Server number
        pub async fn get_shuffle_server_num(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleServerNumResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/getShuffleServerNum",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "getShuffleServerNum",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Ask for suitable Shuffle Servers with partitions
        pub async fn get_shuffle_assignments(
            &mut self,
            request: impl tonic::IntoRequest<super::GetShuffleServerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleAssignmentsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/getShuffleAssignments",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "getShuffleAssignments",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Heartbeat between Shuffle Server and Coordinator Server
        pub async fn heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::ShuffleServerHeartBeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleServerHeartBeatResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/heartbeat",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.CoordinatorServer", "heartbeat"));
            self.inner.unary(req, path, codec).await
        }
        /// Get the global configuration of this Rss-cluster, i.e., data storage info
        pub async fn get_shuffle_data_storage_info(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleDataStorageInfoResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/getShuffleDataStorageInfo",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "getShuffleDataStorageInfo",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn check_service_available(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<
            tonic::Response<super::CheckServiceAvailableResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/checkServiceAvailable",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "checkServiceAvailable",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Heartbeat between Shuffle Application and Coordinator Server
        pub async fn app_heartbeat(
            &mut self,
            request: impl tonic::IntoRequest<super::AppHeartBeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AppHeartBeatResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/appHeartbeat",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("rss.common.CoordinatorServer", "appHeartbeat"));
            self.inner.unary(req, path, codec).await
        }
        /// Report a client operation's result to coordinator server
        pub async fn report_client_operation(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportShuffleClientOpRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportShuffleClientOpResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/reportClientOperation",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "reportClientOperation",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Report a application info to Coordinator Server
        pub async fn register_application_info(
            &mut self,
            request: impl tonic::IntoRequest<super::ApplicationInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ApplicationInfoResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/registerApplicationInfo",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.CoordinatorServer",
                        "registerApplicationInfo",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Access to the remote shuffle service cluster
        pub async fn access_cluster(
            &mut self,
            request: impl tonic::IntoRequest<super::AccessClusterRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AccessClusterResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/accessCluster",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.CoordinatorServer", "accessCluster"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Get basic client conf from coordinator
        pub async fn fetch_client_conf(
            &mut self,
            request: impl tonic::IntoRequest<()>,
        ) -> std::result::Result<
            tonic::Response<super::FetchClientConfResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/fetchClientConf",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.CoordinatorServer", "fetchClientConf"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Get remote storage from coordinator
        pub async fn fetch_remote_storage(
            &mut self,
            request: impl tonic::IntoRequest<super::FetchRemoteStorageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FetchRemoteStorageResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.CoordinatorServer/fetchRemoteStorage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.CoordinatorServer", "fetchRemoteStorage"),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod shuffle_server_internal_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct ShuffleServerInternalClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShuffleServerInternalClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ShuffleServerInternalClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ShuffleServerInternalClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ShuffleServerInternalClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn decommission(
            &mut self,
            request: impl tonic::IntoRequest<super::DecommissionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DecommissionResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServerInternal/decommission",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("rss.common.ShuffleServerInternal", "decommission"),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn cancel_decommission(
            &mut self,
            request: impl tonic::IntoRequest<super::CancelDecommissionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CancelDecommissionResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleServerInternal/cancelDecommission",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.ShuffleServerInternal",
                        "cancelDecommission",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated client implementations.
pub mod shuffle_manager_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// ShuffleManager service lives inside of compute-engine's application master, which handles rss shuffle specific logic
    /// per application.
    #[derive(Debug, Clone)]
    pub struct ShuffleManagerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ShuffleManagerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ShuffleManagerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ShuffleManagerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ShuffleManagerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn report_shuffle_fetch_failure(
            &mut self,
            request: impl tonic::IntoRequest<super::ReportShuffleFetchFailureRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportShuffleFetchFailureResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/rss.common.ShuffleManager/reportShuffleFetchFailure",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "rss.common.ShuffleManager",
                        "reportShuffleFetchFailure",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod shuffle_server_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ShuffleServerServer.
    #[async_trait]
    pub trait ShuffleServer: Send + Sync + 'static {
        async fn register_shuffle(
            &self,
            request: tonic::Request<super::ShuffleRegisterRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleRegisterResponse>,
            tonic::Status,
        >;
        async fn unregister_shuffle(
            &self,
            request: tonic::Request<super::ShuffleUnregisterRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleUnregisterResponse>,
            tonic::Status,
        >;
        async fn send_shuffle_data(
            &self,
            request: tonic::Request<super::SendShuffleDataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::SendShuffleDataResponse>,
            tonic::Status,
        >;
        async fn get_local_shuffle_index(
            &self,
            request: tonic::Request<super::GetLocalShuffleIndexRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLocalShuffleIndexResponse>,
            tonic::Status,
        >;
        async fn get_local_shuffle_data(
            &self,
            request: tonic::Request<super::GetLocalShuffleDataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetLocalShuffleDataResponse>,
            tonic::Status,
        >;
        async fn get_memory_shuffle_data(
            &self,
            request: tonic::Request<super::GetMemoryShuffleDataRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetMemoryShuffleDataResponse>,
            tonic::Status,
        >;
        async fn commit_shuffle_task(
            &self,
            request: tonic::Request<super::ShuffleCommitRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleCommitResponse>,
            tonic::Status,
        >;
        async fn report_shuffle_result(
            &self,
            request: tonic::Request<super::ReportShuffleResultRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportShuffleResultResponse>,
            tonic::Status,
        >;
        async fn get_shuffle_result(
            &self,
            request: tonic::Request<super::GetShuffleResultRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleResultResponse>,
            tonic::Status,
        >;
        async fn get_shuffle_result_for_multi_part(
            &self,
            request: tonic::Request<super::GetShuffleResultForMultiPartRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleResultForMultiPartResponse>,
            tonic::Status,
        >;
        async fn finish_shuffle(
            &self,
            request: tonic::Request<super::FinishShuffleRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FinishShuffleResponse>,
            tonic::Status,
        >;
        async fn require_buffer(
            &self,
            request: tonic::Request<super::RequireBufferRequest>,
        ) -> std::result::Result<
            tonic::Response<super::RequireBufferResponse>,
            tonic::Status,
        >;
        async fn app_heartbeat(
            &self,
            request: tonic::Request<super::AppHeartBeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AppHeartBeatResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ShuffleServerServer<T: ShuffleServer> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ShuffleServer> ShuffleServerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ShuffleServerServer<T>
    where
        T: ShuffleServer,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/rss.common.ShuffleServer/registerShuffle" => {
                    #[allow(non_camel_case_types)]
                    struct registerShuffleSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::ShuffleRegisterRequest>
                    for registerShuffleSvc<T> {
                        type Response = super::ShuffleRegisterResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ShuffleRegisterRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).register_shuffle(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = registerShuffleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/unregisterShuffle" => {
                    #[allow(non_camel_case_types)]
                    struct unregisterShuffleSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::ShuffleUnregisterRequest>
                    for unregisterShuffleSvc<T> {
                        type Response = super::ShuffleUnregisterResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ShuffleUnregisterRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).unregister_shuffle(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = unregisterShuffleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/sendShuffleData" => {
                    #[allow(non_camel_case_types)]
                    struct sendShuffleDataSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::SendShuffleDataRequest>
                    for sendShuffleDataSvc<T> {
                        type Response = super::SendShuffleDataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::SendShuffleDataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).send_shuffle_data(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = sendShuffleDataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/getLocalShuffleIndex" => {
                    #[allow(non_camel_case_types)]
                    struct getLocalShuffleIndexSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::GetLocalShuffleIndexRequest>
                    for getLocalShuffleIndexSvc<T> {
                        type Response = super::GetLocalShuffleIndexResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetLocalShuffleIndexRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_local_shuffle_index(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getLocalShuffleIndexSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/getLocalShuffleData" => {
                    #[allow(non_camel_case_types)]
                    struct getLocalShuffleDataSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::GetLocalShuffleDataRequest>
                    for getLocalShuffleDataSvc<T> {
                        type Response = super::GetLocalShuffleDataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetLocalShuffleDataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_local_shuffle_data(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getLocalShuffleDataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/getMemoryShuffleData" => {
                    #[allow(non_camel_case_types)]
                    struct getMemoryShuffleDataSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::GetMemoryShuffleDataRequest>
                    for getMemoryShuffleDataSvc<T> {
                        type Response = super::GetMemoryShuffleDataResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetMemoryShuffleDataRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_memory_shuffle_data(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getMemoryShuffleDataSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/commitShuffleTask" => {
                    #[allow(non_camel_case_types)]
                    struct commitShuffleTaskSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::ShuffleCommitRequest>
                    for commitShuffleTaskSvc<T> {
                        type Response = super::ShuffleCommitResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ShuffleCommitRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).commit_shuffle_task(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = commitShuffleTaskSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/reportShuffleResult" => {
                    #[allow(non_camel_case_types)]
                    struct reportShuffleResultSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::ReportShuffleResultRequest>
                    for reportShuffleResultSvc<T> {
                        type Response = super::ReportShuffleResultResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReportShuffleResultRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).report_shuffle_result(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = reportShuffleResultSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/getShuffleResult" => {
                    #[allow(non_camel_case_types)]
                    struct getShuffleResultSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::GetShuffleResultRequest>
                    for getShuffleResultSvc<T> {
                        type Response = super::GetShuffleResultResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetShuffleResultRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_shuffle_result(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getShuffleResultSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/getShuffleResultForMultiPart" => {
                    #[allow(non_camel_case_types)]
                    struct getShuffleResultForMultiPartSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<
                        super::GetShuffleResultForMultiPartRequest,
                    > for getShuffleResultForMultiPartSvc<T> {
                        type Response = super::GetShuffleResultForMultiPartResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::GetShuffleResultForMultiPartRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_shuffle_result_for_multi_part(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getShuffleResultForMultiPartSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/finishShuffle" => {
                    #[allow(non_camel_case_types)]
                    struct finishShuffleSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::FinishShuffleRequest>
                    for finishShuffleSvc<T> {
                        type Response = super::FinishShuffleResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FinishShuffleRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).finish_shuffle(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = finishShuffleSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/requireBuffer" => {
                    #[allow(non_camel_case_types)]
                    struct requireBufferSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::RequireBufferRequest>
                    for requireBufferSvc<T> {
                        type Response = super::RequireBufferResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::RequireBufferRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).require_buffer(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = requireBufferSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServer/appHeartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct appHeartbeatSvc<T: ShuffleServer>(pub Arc<T>);
                    impl<
                        T: ShuffleServer,
                    > tonic::server::UnaryService<super::AppHeartBeatRequest>
                    for appHeartbeatSvc<T> {
                        type Response = super::AppHeartBeatResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AppHeartBeatRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).app_heartbeat(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = appHeartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ShuffleServer> Clone for ShuffleServerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ShuffleServer> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ShuffleServer> tonic::server::NamedService for ShuffleServerServer<T> {
        const NAME: &'static str = "rss.common.ShuffleServer";
    }
}
/// Generated server implementations.
pub mod coordinator_server_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with CoordinatorServerServer.
    #[async_trait]
    pub trait CoordinatorServer: Send + Sync + 'static {
        /// Get Shuffle Server list
        async fn get_shuffle_server_list(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleServerListResponse>,
            tonic::Status,
        >;
        /// Count Shuffle Server number
        async fn get_shuffle_server_num(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleServerNumResponse>,
            tonic::Status,
        >;
        /// Ask for suitable Shuffle Servers with partitions
        async fn get_shuffle_assignments(
            &self,
            request: tonic::Request<super::GetShuffleServerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleAssignmentsResponse>,
            tonic::Status,
        >;
        /// Heartbeat between Shuffle Server and Coordinator Server
        async fn heartbeat(
            &self,
            request: tonic::Request<super::ShuffleServerHeartBeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ShuffleServerHeartBeatResponse>,
            tonic::Status,
        >;
        /// Get the global configuration of this Rss-cluster, i.e., data storage info
        async fn get_shuffle_data_storage_info(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<
            tonic::Response<super::GetShuffleDataStorageInfoResponse>,
            tonic::Status,
        >;
        async fn check_service_available(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<
            tonic::Response<super::CheckServiceAvailableResponse>,
            tonic::Status,
        >;
        /// Heartbeat between Shuffle Application and Coordinator Server
        async fn app_heartbeat(
            &self,
            request: tonic::Request<super::AppHeartBeatRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AppHeartBeatResponse>,
            tonic::Status,
        >;
        /// Report a client operation's result to coordinator server
        async fn report_client_operation(
            &self,
            request: tonic::Request<super::ReportShuffleClientOpRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportShuffleClientOpResponse>,
            tonic::Status,
        >;
        /// Report a application info to Coordinator Server
        async fn register_application_info(
            &self,
            request: tonic::Request<super::ApplicationInfoRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ApplicationInfoResponse>,
            tonic::Status,
        >;
        /// Access to the remote shuffle service cluster
        async fn access_cluster(
            &self,
            request: tonic::Request<super::AccessClusterRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AccessClusterResponse>,
            tonic::Status,
        >;
        /// Get basic client conf from coordinator
        async fn fetch_client_conf(
            &self,
            request: tonic::Request<()>,
        ) -> std::result::Result<
            tonic::Response<super::FetchClientConfResponse>,
            tonic::Status,
        >;
        /// Get remote storage from coordinator
        async fn fetch_remote_storage(
            &self,
            request: tonic::Request<super::FetchRemoteStorageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::FetchRemoteStorageResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct CoordinatorServerServer<T: CoordinatorServer> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: CoordinatorServer> CoordinatorServerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for CoordinatorServerServer<T>
    where
        T: CoordinatorServer,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/rss.common.CoordinatorServer/getShuffleServerList" => {
                    #[allow(non_camel_case_types)]
                    struct getShuffleServerListSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<T: CoordinatorServer> tonic::server::UnaryService<()>
                    for getShuffleServerListSvc<T> {
                        type Response = super::GetShuffleServerListResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_shuffle_server_list(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getShuffleServerListSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/getShuffleServerNum" => {
                    #[allow(non_camel_case_types)]
                    struct getShuffleServerNumSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<T: CoordinatorServer> tonic::server::UnaryService<()>
                    for getShuffleServerNumSvc<T> {
                        type Response = super::GetShuffleServerNumResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_shuffle_server_num(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getShuffleServerNumSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/getShuffleAssignments" => {
                    #[allow(non_camel_case_types)]
                    struct getShuffleAssignmentsSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::GetShuffleServerRequest>
                    for getShuffleAssignmentsSvc<T> {
                        type Response = super::GetShuffleAssignmentsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetShuffleServerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_shuffle_assignments(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getShuffleAssignmentsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/heartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct heartbeatSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::ShuffleServerHeartBeatRequest>
                    for heartbeatSvc<T> {
                        type Response = super::ShuffleServerHeartBeatResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ShuffleServerHeartBeatRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move { (*inner).heartbeat(request).await };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = heartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/getShuffleDataStorageInfo" => {
                    #[allow(non_camel_case_types)]
                    struct getShuffleDataStorageInfoSvc<T: CoordinatorServer>(
                        pub Arc<T>,
                    );
                    impl<T: CoordinatorServer> tonic::server::UnaryService<()>
                    for getShuffleDataStorageInfoSvc<T> {
                        type Response = super::GetShuffleDataStorageInfoResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).get_shuffle_data_storage_info(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = getShuffleDataStorageInfoSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/checkServiceAvailable" => {
                    #[allow(non_camel_case_types)]
                    struct checkServiceAvailableSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<T: CoordinatorServer> tonic::server::UnaryService<()>
                    for checkServiceAvailableSvc<T> {
                        type Response = super::CheckServiceAvailableResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).check_service_available(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = checkServiceAvailableSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/appHeartbeat" => {
                    #[allow(non_camel_case_types)]
                    struct appHeartbeatSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::AppHeartBeatRequest>
                    for appHeartbeatSvc<T> {
                        type Response = super::AppHeartBeatResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AppHeartBeatRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).app_heartbeat(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = appHeartbeatSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/reportClientOperation" => {
                    #[allow(non_camel_case_types)]
                    struct reportClientOperationSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::ReportShuffleClientOpRequest>
                    for reportClientOperationSvc<T> {
                        type Response = super::ReportShuffleClientOpResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ReportShuffleClientOpRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).report_client_operation(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = reportClientOperationSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/registerApplicationInfo" => {
                    #[allow(non_camel_case_types)]
                    struct registerApplicationInfoSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::ApplicationInfoRequest>
                    for registerApplicationInfoSvc<T> {
                        type Response = super::ApplicationInfoResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ApplicationInfoRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).register_application_info(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = registerApplicationInfoSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/accessCluster" => {
                    #[allow(non_camel_case_types)]
                    struct accessClusterSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::AccessClusterRequest>
                    for accessClusterSvc<T> {
                        type Response = super::AccessClusterResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AccessClusterRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).access_cluster(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = accessClusterSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/fetchClientConf" => {
                    #[allow(non_camel_case_types)]
                    struct fetchClientConfSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<T: CoordinatorServer> tonic::server::UnaryService<()>
                    for fetchClientConfSvc<T> {
                        type Response = super::FetchClientConfResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(&mut self, request: tonic::Request<()>) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).fetch_client_conf(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = fetchClientConfSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.CoordinatorServer/fetchRemoteStorage" => {
                    #[allow(non_camel_case_types)]
                    struct fetchRemoteStorageSvc<T: CoordinatorServer>(pub Arc<T>);
                    impl<
                        T: CoordinatorServer,
                    > tonic::server::UnaryService<super::FetchRemoteStorageRequest>
                    for fetchRemoteStorageSvc<T> {
                        type Response = super::FetchRemoteStorageResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FetchRemoteStorageRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).fetch_remote_storage(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = fetchRemoteStorageSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: CoordinatorServer> Clone for CoordinatorServerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: CoordinatorServer> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: CoordinatorServer> tonic::server::NamedService
    for CoordinatorServerServer<T> {
        const NAME: &'static str = "rss.common.CoordinatorServer";
    }
}
/// Generated server implementations.
pub mod shuffle_server_internal_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ShuffleServerInternalServer.
    #[async_trait]
    pub trait ShuffleServerInternal: Send + Sync + 'static {
        async fn decommission(
            &self,
            request: tonic::Request<super::DecommissionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DecommissionResponse>,
            tonic::Status,
        >;
        async fn cancel_decommission(
            &self,
            request: tonic::Request<super::CancelDecommissionRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CancelDecommissionResponse>,
            tonic::Status,
        >;
    }
    #[derive(Debug)]
    pub struct ShuffleServerInternalServer<T: ShuffleServerInternal> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ShuffleServerInternal> ShuffleServerInternalServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for ShuffleServerInternalServer<T>
    where
        T: ShuffleServerInternal,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/rss.common.ShuffleServerInternal/decommission" => {
                    #[allow(non_camel_case_types)]
                    struct decommissionSvc<T: ShuffleServerInternal>(pub Arc<T>);
                    impl<
                        T: ShuffleServerInternal,
                    > tonic::server::UnaryService<super::DecommissionRequest>
                    for decommissionSvc<T> {
                        type Response = super::DecommissionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DecommissionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).decommission(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = decommissionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/rss.common.ShuffleServerInternal/cancelDecommission" => {
                    #[allow(non_camel_case_types)]
                    struct cancelDecommissionSvc<T: ShuffleServerInternal>(pub Arc<T>);
                    impl<
                        T: ShuffleServerInternal,
                    > tonic::server::UnaryService<super::CancelDecommissionRequest>
                    for cancelDecommissionSvc<T> {
                        type Response = super::CancelDecommissionResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CancelDecommissionRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).cancel_decommission(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = cancelDecommissionSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ShuffleServerInternal> Clone for ShuffleServerInternalServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ShuffleServerInternal> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ShuffleServerInternal> tonic::server::NamedService
    for ShuffleServerInternalServer<T> {
        const NAME: &'static str = "rss.common.ShuffleServerInternal";
    }
}
/// Generated server implementations.
pub mod shuffle_manager_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ShuffleManagerServer.
    #[async_trait]
    pub trait ShuffleManager: Send + Sync + 'static {
        async fn report_shuffle_fetch_failure(
            &self,
            request: tonic::Request<super::ReportShuffleFetchFailureRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ReportShuffleFetchFailureResponse>,
            tonic::Status,
        >;
    }
    /// ShuffleManager service lives inside of compute-engine's application master, which handles rss shuffle specific logic
    /// per application.
    #[derive(Debug)]
    pub struct ShuffleManagerServer<T: ShuffleManager> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: ShuffleManager> ShuffleManagerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ShuffleManagerServer<T>
    where
        T: ShuffleManager,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/rss.common.ShuffleManager/reportShuffleFetchFailure" => {
                    #[allow(non_camel_case_types)]
                    struct reportShuffleFetchFailureSvc<T: ShuffleManager>(pub Arc<T>);
                    impl<
                        T: ShuffleManager,
                    > tonic::server::UnaryService<
                        super::ReportShuffleFetchFailureRequest,
                    > for reportShuffleFetchFailureSvc<T> {
                        type Response = super::ReportShuffleFetchFailureResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::ReportShuffleFetchFailureRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                (*inner).report_shuffle_fetch_failure(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = reportShuffleFetchFailureSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: ShuffleManager> Clone for ShuffleManagerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    impl<T: ShuffleManager> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(Arc::clone(&self.0))
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: ShuffleManager> tonic::server::NamedService for ShuffleManagerServer<T> {
        const NAME: &'static str = "rss.common.ShuffleManager";
    }
}
