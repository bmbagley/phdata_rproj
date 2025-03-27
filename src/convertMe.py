"""
convertMe.py
~~~

Converted convertMe.r file to pyspark
"""

from utils import get_proj_root
import pathlib

def main():
    print(pathlib.Path().cwd())
    return get_proj_root() 

if __name__=='__main__':
    print(main())
# print(main)