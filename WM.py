#!/usr/bin/env python3

import sys
import subprocess
import importlib

choices = subprocess.run(["ls", "WM_*"],
                         capture_output=True,
                         text=True).stdout.split()
choices = [
    c.removesuffix(".py")
    for c in choices
]
pyfile = sys.argv[1] if len(sys.argv) > 1 else ""

while pyfile not in choices:
    pyfile = input(f"Select a correct deploy choice {choices}\n> ")

mod = importlib.import_module(pyfile)
mod.main(*sys.argv[1:])
