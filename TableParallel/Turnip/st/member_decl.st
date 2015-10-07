member_decl(membser) ::= <<

$if(membser.CharacterMaximumLength)$
  [StringLength($membser.CharacterMaximumLength$)]
$endif$
  public $membser.DataType$ $membser.Name$ { get; set; }

>>

