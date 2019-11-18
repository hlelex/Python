"""
Search directory tree for all duplicated files
Встроенный LRU-кэш (3.2+)

В наши дни механизмы кэширования применяются практически во всех программных и аппаратных системах. Python 3 значительно
 упрощает кэширование благодаря декоратору lru_cache, который реализует алгоритм LRU-кэширования (Least Recently Used).

Ниже показана функция, которая вычисляет числа Фибоначчи. Эта функция вынуждена по много раз выполнять одни и те же
операции в ходе рекурсивных вызовов. В результате оказывается, что её производительность можно улучшить благодаря
кэшированию.

import time
def fib(number: int) -> int:
    if number == 0: return 0
    if number == 1: return 1

    return fib(number-1) + fib(number-2)
start = time.time()
fib(40)
print(f'Duration: {time.time() - start}s')
# Duration: 30.684099674224854s

Теперь используем lru_cache для оптимизации этой функции (такая техника оптимизации называется мемоизацией).
В результате время выполнения функции, которое раньше измерялось секундами, теперь измеряется наносекундами.

from functools import lru_cache
@lru_cache(maxsize=512)
def fib_memoization(number: int) -> int:
    if number == 0: return 0
    if number == 1: return 1

    return fib_memoization(number-1) + fib_memoization(number-2)
start = time.time()
fib_memoization(40)
print(f'Duration: {time.time() - start}s')
# Duration: 6.866455078125e-05s
"""
# coding=utf-8
import os
# import subprocess
import time
from time import strftime
import sys
# import multiprocessing
import dataset
import hashlib
# import json
from concurrent import futures
# import pickle
from pprint import pprint
# import re
# from mutagen.flac import FLAC
import filetype
from functools import lru_cache


# import asyncio
# import aiofiles

# wav = 'myfile.wav'
# cmd = "C:\Program Files (x86)\Exact Audio Copy\Flac\Flac"
# cmd=r"C:\Users\Olex\AppData\Local\Programs\Python\Python36-32\Scripts\beet"
# subprocess.run(cmd, shell=True)
# from functools import lru_cache


def audiofilescheck(argvs):
    # audio = FLAC(r'D:\Music\14 - Close To You.flac')
    # audio["title"] = u"An --- exam Close To You"
    # audio.pprint()
    # pprint(audio.items())
    # audio.save()
    # exit()
    # =================================================================================================================
    #  Buffer | 32768  | 1048576 | 5242880 | 10485760 | 15728640 | 52428800  | 104857600 | 157286400 | ‭209715200 |
    #   Size  |  32K   |  1Mb    |   5Mb   |   10Mb   |   15Mb   |   51Mb(32)|   102.4Mb |   153.6Mb |   200Mb   | 1Gb
    #         | 0x8000 | 0xFA000 | 0x4E2000| 0x9C4000 | 0xEA6000 | 0x30D4000 | 0x61A8000 | 0x927C000 | 0x‭C350000‬ |
    # --------|--------|---------|---------|----------|----------|-----------|-----------------------------------------
    # CPUs'x1 |  137   |   44    | 33.25   |   31     |  29.7    | 27.1(27.6)|    29     |    31     |    35     | 104
    #   x2    |  178   |   101   | 34.41   |  30.68   |  30.47   |    28     |   28.8    |   30.21   |    33     |
    # ========|========|=========|=========|==========|==========|=====================================================

    OS_CPU_N = os.cpu_count()
    # BUFF_SIZE = 0x30D4000  # 49Mb, 28.97s # Automatic cash - 26.7
    # BUFF_SIZE = 0x8000
    totflist = []

    # excludes = ('desktop.ini', 'thumbs.db')  # Files excluded from the process
    hashmap = {}  # content signature -> list of file names
    t0 = time.time()  # Start time marker
    mypath = r''.join(map(str, argvs[0:1]))
    mypath = r'D:\Music'
    # path = r'K:\alex\Test CDs'
    # path = r'Z:\Audio Music\FLACs\10CC'
    # path = r'Z:\Audio Music\FLACs\1988 Charlie Parker'
    # path = r'E:\Electric Light Orchestra - Discography'
    # path = r'N:\Audio Music\FLACs\10CC'

    print('Start time:', strftime('%H:%M:%S'))
    print('CPUs : ', OS_CPU_N)
    # print(len(argvs), argvs[:], 'Path: ', path)
    print('Path: ', mypath)

    def hash_calc():
        def hash_calc_in(fullname):
            print('File processing: ', fullname)
            h = hashlib.md5()
            with open(fullname, 'r+b') as f:
                h.update(f.read())
                # while True:
                #    read_data = f.read()
                #    if not read_data:
                #        break
                #    h.update(read_data)

                # hashlib.md5(f.read()).hexdigest()
            #    d = f.read()
            # h.update(d)
            filelst = hashmap.setdefault(h.hexdigest(), [])
            filelst.append(fullname)
            print(' Done: ', fullname)

        with futures.ThreadPoolExecutor() as e:  # Start multi
            e.map(hash_calc_in, totflist)
            # for _ in fullname:
            #    e.submit(hash_calc_in, _)

    # ------------------- START ----------------------
    def read_allfiles():
        for path, dirs, files in os.walk(mypath):
            for filename in files:
                # if filename.lower() in excludes:
                #    continue
                fullname_tmp = os.path.join(path, filename)
                # if not os.path.getsize(path + '\\' + filename):  # zero files check (True=1 False=0)
                if not os.path.getsize(fullname_tmp):  # zero-size files check skip (True=1 False=0)
                    continue
                with open(fullname_tmp, 'r+b') as file:
                    # info = fleep.get(file.read(0x80))
                    flinfo = filetype.guess(file)
                if str(flinfo).find('audio') == (-1):
                    # print('Skiped -> ', os.path.join(path, filename))
                    continue
                # fullname_tmp = os.path.join(path, filename)
                if '\xb4' in fullname_tmp:
                    fullname = fullname_tmp.replace('\xb4', '\x27')
                    os.rename(fullname_tmp, fullname)
                    print('File renamed: ' + fullname_tmp + ' ----> ' + fullname)
                    totflist.append(fullname)
                else:
                    totflist.append(fullname_tmp)

    def ttm(t):
        msg = '{:.2f} s'
        print('End time:', strftime('%H:%M:%S'), ' ( The program has taken ', msg.format(t), ')')

    read_allfiles()
    hash_calc()

    if not hashmap:
        exit('Path \'' + mypath + '\' or any appropriate file(s) not found.')

    pprint(list(hashmap.items()), depth=3, width=360)

    with open('AllFiles.lst', 'w') as f:  # write list of the files to the one txt(lst)-file
        for m in hashmap.keys():
            print(str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), file=f)

    print('-Start DB- ' + strftime('%H:%M:%S'))
    t1 = time.time()

    """
    db = dataset.connect('sqlite:///Allfiles.db')
    if db['Files']:
        print('The previous version of database has found and will be updated.')
        db.begin()
        for m in db['Files']:  # Cleanup database
            if m['md5'] not in hashmap.keys():
                db['Files'].delete()
                print('- record deleted: ', m['path'])
    """
    with dataset.connect('sqlite:///Allfiles.db') as tx:
        # print('The previous version of database has found and will be updated.')
        for m in hashmap.keys():  # Update database
            key = 0
            if "', '" in str(hashmap[m]): key = 1
            tx['Files'].upsert(dict(md5=m, path=str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), dups=key),
                               ['md5'])
        for m in tx['Files']:  # Cleanup database
            if m['md5'] not in hashmap.keys():
                tx['Files'].delete()
                print('- record deleted: ', m['path'])
    ttm(time.time() - t1)
    # ttm(time.time() - t1)
    """
    def dup_files(key):  # key - Boolen : True = duplicated
        with dataset.connect('sqlite:///Allfiles.db') as tx:
            tx['Files'].upsert(dict(md5=m, path=str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), dups=key), ['md5'])

    #-def dup_files(key):  # key - Boolen : True = duplicated
    #-    db['Files'].upsert(dict(md5=m, path=str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), dups=key), ['md5'])
        # db['Files'].insert_many([dict(md5=m, path=str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), dups=key)])

    #for m in hashmap.keys():  # Update database
    #    if "', '" in str(hashmap[m]):
    #        dup_files(True)
    #    else:
    #        dup_files(False)
    #db.commit()
    """
    print('Total files in the list for calculation: ', len(totflist))
    print('Records in the database file:', len(hashmap))
    print('Duplicated record(s) found:')
    with dataset.connect('sqlite:///Allfiles.db') as tx:
        for t in tx['Files'].find(dups=True):
            print('\n'.join(str(t['path']).replace('\'', '').split(', ')) + ' ---> ' + str(t['md5']))


#   -------- Pickle -----------
#   with open('AllFiles.pkl', 'wb') as f:
#    pickle.dump(hashmap, f)

if __name__ == "__main__":
    audiofilescheck(sys.argv[1:])
