namespace_decl(usings, namespaceName, classList) ::= <<

/* This is generated from Turnip.ST. */
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
$usings:{ u | using $u$; }$


namespace $namespaceName$
{

$classList:{ c |

  $class_decl(c.TableName, c.Name, c.Members, c.BaseClass)$

}$
}
>>
