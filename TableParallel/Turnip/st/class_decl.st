class_decl(tableName, name, members, baseClass) ::= <<

[System.Diagnostics.DebuggerNonUserCode]
[System.Runtime.CompilerServices.CompilerGenerated]
[Table("$tableName$")]
public partial class $name$$if(baseClass)$ : $baseClass$$endif$
{
  $members:{ m | $member_decl(m)$ }$
}
>>


