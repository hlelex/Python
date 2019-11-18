# Search directory tree for all duplicate files
# coding=utf-8
import os
# import subprocess
import time
from time import strftime
import sys
#import multiprocessing
import dataset
import hashlib
# import json
from concurrent import futures
# import pickle
from pprint import pprint
# import re
from mutagen.flac import FLAC


# wav = 'myfile.wav'
# cmd = "C:\Program Files (x86)\Exact Audio Copy\Flac\Flac"
# cmd=r"C:\Users\Olex\AppData\Local\Programs\Python\Python36-32\Scripts\beet"
# subprocess.run(cmd, shell=True)


def main(argvs):
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
    BUFF_SIZE = 0x2EE0000  # 49Mb
    totflist = []

    excludes = ('desktop.ini', 'thumbs.db')  # Files excluded from the process
    hashmap = {}  # content signature -> list of file names
    t0 = time.time()  # Start time marker
    path = r''.join(map(str, argvs[0:1]))
    # path = r'K:\alex\Test CDs'
    # path = r'Z:\Audio Music\FLACs\10CC'
    # path = r'Z:\Audio Music\FLACs\1988 Charlie Parker'

    # ------------- Start ------------

    print('Start time:', strftime('%H:%M:%S'))
    print('CPUs : ', OS_CPU_N)
    print(len(argvs), argvs[:])

    def hash_calc(fullname):
        def hash_calc_in(fullname):
            print('File processing: ', fullname)
            h = hashlib.md5()
            with open(fullname, 'rb') as f:
                d = f.read(BUFF_SIZE)
                while d:
                    h.update(d)
                    d = f.read(BUFF_SIZE)
            filelst = hashmap.setdefault(h.hexdigest(), [])
            filelst.append(fullname)
            print(' Done: ', fullname)

        with futures.ThreadPoolExecutor(OS_CPU_N) as e:
            for _ in fullname:
                e.submit(hash_calc_in, _)

    for path, dirs, files in os.walk(path):
        for filename in files:
            if filename.lower() in excludes:
                continue
            if not os.path.getsize(path + '\\' + filename):  # zero files check (True=1 False=0)
                continue
            fullname_tmp = os.path.join(path, filename)
            if '\xb4' in fullname_tmp:
                fullname = fullname_tmp.replace('\xb4', '\x27')
                os.rename(fullname_tmp, fullname)
                print('File renamed: ' + fullname_tmp + ' ----> ' + fullname)
                totflist.append(fullname)
            else:
                totflist.append(fullname_tmp)

    hash_calc(totflist)

    if not hashmap:
        exit('Path to files not found.')

    print(len(totflist))
    pprint(list(hashmap.items()), depth=3, width=350)
    print('Records/files processed:', len(hashmap))

    with open('AllFiles.lst', 'w') as f:  # write list of the files to the one txt(lst)-file
        for m in hashmap.keys():
            print(str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), file=f)

    db = dataset.connect('sqlite:///Allfiles.db')
    if db['Files']:
        print('The previous version of database has found and will be updated.')
        db.begin()
        for m in db['Files']:  # Cleanup database
            if m['md5'] not in hashmap.keys():
                db['Files'].delete()
                print('- record deleted: ', m['path'])

    def dup_files(key):  # key - Boolen : True = duplicated
        db['Files'].upsert(dict(md5=m, path=str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), dups=key), ['md5'])

    for m in hashmap.keys():  # Update database
        if "', '" in str(hashmap[m]):
            dup_files(True)
        else:
            dup_files(False)
    db.commit()

    def ttm(t):
        msg = '{:.2f} s'
        print('End time:', strftime('%H:%M:%S'), ' ( The program has taken ', msg.format(t), ')')

    ttm(time.time() - t0)

    print('Duplicated record(s) found:')
    for t in db['Files'].find(dups=True):
        print(t['path'])


#   -------- Pickle -----------
#   with open('AllFiles.pkl', 'wb') as f:
#    pickle.dump(hashmap, f)


if __name__ == "__main__":
    main(sys.argv[1:])
