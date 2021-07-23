#!/usr/bin/python3

import os.path

print('Content-Type: text/html')
print()
print('<title>Remove File</title>')
print('<h1>Remove File Module</h1>')

if os.path.exists('stop.txt'):
    os.remove('stop.txt')
    print('File deleted')

if not os.path.exists('stop.txt'):
    print('file not found. file deleted')
elif os.path.exists('stop.txt'):
    print('file not deleted')

