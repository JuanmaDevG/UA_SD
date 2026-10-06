#!/usr/bin/env python3

import os
import sys
import subprocess

choices = subprocess.run(["ls", "WM_*"],
                         capture_output=True,
                         text=True).stdout.split()
choices = [
    c.removesuffix(".py").removeprefix("WM_")
    for c in choices
]
pyfile = sys.argv[1] if len(sys.argv) > 1 else ""

while pyfile not in choices:
    pyfile = input(f"Select a correct deploy choice {choices}\n> ")
pyfile = "WM_" + pyfile + ".py"
os.execvp(sys.executable, [sys.executable, pyfile])
