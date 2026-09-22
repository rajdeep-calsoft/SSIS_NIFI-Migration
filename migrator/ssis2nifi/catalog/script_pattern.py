"""Recognise a small, bounded set of Script Component idioms from their
actual C# source, the same "bounded subset, refuse the rest" shape
catalog/expr.py uses for SSIS expressions.

WHY THIS EXISTS, AND WHY IT IS NOT "COMPILING ARBITRARY C#"
-------------------------------------------------------------
A Script Component (`Microsoft.ManagedComponentHost` /
`Microsoft.ScriptComponentHost`) can contain genuinely arbitrary .NET code --
that is why catalog/support.py refuses it outright by default, and that
default is correct: interpreting arbitrary code at compile time would make
this tool a guesser, not a compiler (see catalog/support.py's module
docstring).

What this module does instead is much narrower: it looks at the ONE method
SSIS always generates a fixed wrapper around (`Input0_ProcessInputRow`,
named by the auto-generated `ComponentWrapper.cs`, not by the package
author -- see any real corpus file's SourceCode array), strips comments and
whitespace, and checks whether the ENTIRE body is *exactly* one recognised
statement shape. If it is not -- multiple statements, any other API call,
anything the pattern below doesn't name -- this refuses, the same as an
unsupported component would. Recognising a specific known idiom is not the
same claim as "this tool understands C#"; it is the same claim expr.py makes
about SSIS expressions: a bounded grammar, refuse outside it.

CURRENTLY RECOGNISED: exactly one idiom
----------------------------------------
    Row.<Out> = ComponentMetaData.GetErrorDescription(Row.<In>);
    Row.<Out> = this.ComponentMetaData.GetErrorDescription(Row.<In>);

The idiom that appears in this repo's own corpus (converter/corpus/packages/
L4.dtsx, L6.dtsx) and is Microsoft's own documented pattern for attaching a
human-readable description to an error-output row's ErrorCode column -- see
https://learn.microsoft.com/en-us/sql/integration-services/extending-packages-scripting-data-flow-script-component-examples/enhancing-an-error-output-with-the-script-component
GetErrorDescription looks up a code against Integration Services' own
predefined error catalogue, which is NOT closed over arbitrary
third-party-component codes (Microsoft's own docs: "the reference lists all
the errors raised by Integration Services components specifically") -- so the
NiFi-side translation (catalog/derive.py's `_script_component`,
catalogue/components/microsoft.managedcomponenthost.yml) embeds only the
~20 DTSBC_E_* base-data-flow-component codes (the ones a component built on
Microsoft's own base class can raise), sourced verbatim from
https://learn.microsoft.com/en-us/sql/integration-services/integration-services-error-and-message-reference
(catalogue/data/ssis_dataflow_error_codes.json), and leaves any other code's
description blank rather than fabricate one that was never real.
"""
from __future__ import annotations

import re

_METHOD = re.compile(
    r"Input0_ProcessInputRow\s*\(\s*Input0Buffer\s+Row\s*\)\s*\{(.*?)\n\s*\}",
    re.S,
)
_COMMENT = re.compile(r"/\*.*?\*/", re.S)
_LINE_COMMENT = re.compile(r"//[^\n]*")

_GET_ERROR_DESCRIPTION = re.compile(
    r"^Row\.(?P<out_col>\w+)\s*=\s*(?:this\.)?ComponentMetaData\.GetErrorDescription"
    r"\(\s*Row\.(?P<in_col>\w+)\s*\)\s*;$"
)


def recognise_get_error_description(source_files: dict[str, str]) -> tuple[str, str] | None:
    """(out_col, in_col) if `main.cs`'s Input0_ProcessInputRow body is
    *exactly* the GetErrorDescription idiom, else None -- including when the
    file/method isn't found, the body has more than one statement, or the
    one statement doesn't match. None means "refuse", never "guess".
    """
    main = source_files.get("main.cs", "")
    if not main:
        return None

    # SSIS emits the wrapper's own empty override (in ComponentWrapper.cs's
    # copy, sometimes duplicated in main.cs's own base class stub) AND the
    # real user override in ScriptMain -- take the LAST match, which is
    # always the most-derived (user) class's body when both are present.
    matches = list(_METHOD.finditer(main))
    if not matches:
        return None
    body = matches[-1].group(1)

    body = _COMMENT.sub("", body)
    body = _LINE_COMMENT.sub("", body)
    statements = [s.strip() for s in body.strip().splitlines() if s.strip()]
    if len(statements) != 1:
        return None

    m = _GET_ERROR_DESCRIPTION.match(statements[0])
    if not m:
        return None
    return m.group("out_col"), m.group("in_col")
