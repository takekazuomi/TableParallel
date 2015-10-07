/* This is generated from Turnip.ST. */
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.WindowsAzure.Storage.Table; 


namespace TableParallel
{


  [System.Diagnostics.DebuggerNonUserCode]
  [System.Runtime.CompilerServices.CompilerGenerated]
  [Table("SchemasTable")]
  public partial class SchemasTable : TableEntity
  {
      public string PhysicalTableName { get; set; }
     
      public string MD5Hash { get; set; }
     
      public string Schema { get; set; }
     
      public string Uploader { get; set; }
     
      public DateTime UploadTS { get; set; }
     
      public string Reserved1 { get; set; }
     
      public string Reserved2 { get; set; }
     
      public string Reserved3 { get; set; }
     
      public string N { get; set; }
     
      public DateTime TIMESTAMP { get; set; }
     
  }


  [System.Diagnostics.DebuggerNonUserCode]
  [System.Runtime.CompilerServices.CompilerGenerated]
  [Table("WADDiagnosticInfrastructureLogsTable")]
  public partial class WADDiagnosticInfrastructureLogsTable : TableEntity
  {
      public DateTime PreciseTimeStamp { get; set; }
     
      public string DeploymentId { get; set; }
     
      public string Role { get; set; }
     
      public string RoleInstance { get; set; }
     
      public int Level { get; set; }
     
      public string ProviderGuid { get; set; }
     
      public int EventId { get; set; }
     
      public int Pid { get; set; }
     
      public int Tid { get; set; }
     
      public string ActivityId { get; set; }
     
      public string Function { get; set; }
     
      public int Line { get; set; }
     
      public int MDRESULT { get; set; }
     
      public int ErrorCode { get; set; }
     
      public string ErrorCodeMsg { get; set; }
     
      public string Message { get; set; }
     
      public long EventTickCount { get; set; }
     
      public string RowIndex { get; set; }
     
      public DateTime TIMESTAMP { get; set; }
     
  }


  [System.Diagnostics.DebuggerNonUserCode]
  [System.Runtime.CompilerServices.CompilerGenerated]
  [Table("WADPerformanceCountersTable")]
  public partial class WADPerformanceCountersTable : TableEntity
  {
      public DateTime PreciseTimeStamp { get; set; }
     
      public string DeploymentId { get; set; }
     
      public string Role { get; set; }
     
      public string RoleInstance { get; set; }
     
      public string CounterName { get; set; }
     
      public Double CounterValue { get; set; }
     
      public long EventTickCount { get; set; }
     
      public string RowIndex { get; set; }
     
      public DateTime TIMESTAMP { get; set; }
     
  }


  [System.Diagnostics.DebuggerNonUserCode]
  [System.Runtime.CompilerServices.CompilerGenerated]
  [Table("WADWindowsEventLogsTable")]
  public partial class WADWindowsEventLogsTable : TableEntity
  {
      public DateTime PreciseTimeStamp { get; set; }
     
      public string DeploymentId { get; set; }
     
      public string Role { get; set; }
     
      public string RoleInstance { get; set; }
     
      public string ProviderGuid { get; set; }
     
      public string ProviderName { get; set; }
     
      public int EventId { get; set; }
     
      public int Level { get; set; }
     
      public int Pid { get; set; }
     
      public int Tid { get; set; }
     
      public int Opcode { get; set; }
     
      public int Task { get; set; }
     
      public string Channel { get; set; }
     
      public string Description { get; set; }
     
      public string RawXml { get; set; }
     
      public long EventTickCount { get; set; }
     
      public string RowIndex { get; set; }
     
      public DateTime TIMESTAMP { get; set; }
     
  }


}