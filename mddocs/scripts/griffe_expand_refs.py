# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
"""
Griffe extension: expands short cross-references in docstrings,
and resolves __doc__ = Parent.__doc__.replace("X", "Y") assignments.

[shortname][] -> [shortname][full.qualified.path]

Fires on_class_members — at that point all class members are loaded,
so we can resolve short names to full paths for both the class docstring
and all member docstrings.

on_class / on_module resolve __doc__ = Parent.__doc__.replace(...) patterns
that griffe cannot handle statically (they look like Assign, not docstrings).
Resolution is deferred: if the parent class is not yet loaded when on_class
fires, the item is retried in every subsequent on_module call.
"""

import ast as pyast
import re

from griffe import Class, Docstring, Extension, Module

XREF_RE = re.compile(r"\[(\w+)\]\[\]")


class ExpandShortRefs(Extension):
    def __init__(self):
        self._pending = []  # list of (cls, parent_name, from_str, to_str)

    def on_class(self, *, cls, loader, **kwargs):
        if cls.docstring is not None:
            return
        info = _parse_doc_assignment(cls)
        if not info:
            return
        parent_name, from_str, to_str = info
        if not _try_resolve(cls, parent_name, from_str, to_str, loader.modules_collection):
            self._pending.append((cls, parent_name, from_str, to_str))

    def on_module(self, *, mod, loader, **kwargs):
        if not self._pending:
            return
        still_pending = []
        for cls, parent_name, from_str, to_str in self._pending:
            if not _try_resolve(cls, parent_name, from_str, to_str, loader.modules_collection):
                still_pending.append((cls, parent_name, from_str, to_str))
        self._pending = still_pending

    def on_class_members(self, *, cls, **kwargs):
        members = {name: member for name, member in cls.members.items()}
        attributes = {name: attr for name, attr in cls.attributes.items()}

        def replace(m):
            name = m.group(1)
            if name in attributes:
                return f"[{name}][{cls.path}({name})]"
            if name in members:
                return f"[{name}][{members[name].path}]"
            return m.group(0)

        _rewrite(cls.docstring, replace)
        for member in cls.members.values():
            _rewrite(member.docstring, replace)


def _parse_doc_assignment(cls):
    """Parse __doc__ = Parent.__doc__.replace("X", "Y") from class AST source."""
    if cls.filepath is None:
        return None
    try:
        src = cls.filepath.read_text()
        tree = pyast.parse(src)
    except Exception:  # noqa: BLE001
        return None

    for node in pyast.walk(tree):
        if not (isinstance(node, pyast.ClassDef) and node.name == cls.name):
            continue
        for stmt in node.body:
            if not isinstance(stmt, pyast.Assign):
                continue
            if not any(isinstance(t, pyast.Name) and t.id == "__doc__" for t in stmt.targets):
                continue
            call = stmt.value
            if not (
                isinstance(call, pyast.Call)
                and isinstance(call.func, pyast.Attribute)
                and call.func.attr == "replace"
                and isinstance(call.func.value, pyast.Attribute)
                and call.func.value.attr == "__doc__"
                and isinstance(call.func.value.value, pyast.Name)
                and len(call.args) == 2  # noqa: PLR2004
                and all(isinstance(a, pyast.Constant) for a in call.args)
            ):
                return None
            return (
                call.func.value.value.id,
                call.args[0].value,
                call.args[1].value,
            )
    return None


def _try_resolve(cls, parent_name, from_str, to_str, collection):
    parent = next((c for c in _iter_classes(collection) if c.name == parent_name), None)
    if parent is None or parent.docstring is None:
        return False
    new_text = parent.docstring.value.replace(from_str, to_str)
    cls.docstring = Docstring(new_text, parent=cls)
    return True


def _iter_classes(module):
    for member in module.members.values():
        if isinstance(member, Class):
            yield member
        elif isinstance(member, Module):
            yield from _iter_classes(member)


def _rewrite(docstring, replace_fn):
    if docstring is None:
        return
    original = docstring.value
    rewritten = XREF_RE.sub(replace_fn, original)
    if rewritten != original:
        docstring.value = rewritten
