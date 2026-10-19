#!/usr/bin/env python3
"""Launcher:  python WM.py <WM_Central|WM_FO|WM_WS_E|WM_WS_M> [module args...]"""

import importlib
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

# launcher name -> (python module, entry function)
CHOICES = {
    "WM_Central": ("WM_Central", "main"),
    "WM_FO": ("WM_FO", "main"),
    "WM_WS_E": ("WM_WS", "engine"),
    "WM_WS_M": ("WM_WS", "monitor"),
}

# Works with the repo layout (WM_X/WM_X.py) and with the flat container layout
for folder in ("", "WM_Central", "WM_FO", "WM_WS"):
    path = os.path.join(HERE, folder)
    if os.path.isdir(path) and path not in sys.path:
        sys.path.append(path)

name = sys.argv[1] if len(sys.argv) > 1 else ""
while name not in CHOICES:
    if not sys.stdin.isatty():
        sys.exit(f"Unknown module '{name}'. Choices: {list(CHOICES)}")
    name = input(f"Select a correct deploy choice {list(CHOICES)}\n> ").strip()

module, function = CHOICES[name]
sys.exit(getattr(importlib.import_module(module), function)(*sys.argv[2:]))
