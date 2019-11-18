import os
import time
from time import strftime
import sys
import dataset
import hashlib
from concurrent import futures
from pprint import pprint
# import threading
import asyncio
import aiofiles as aiofiles
import filetype

def audiofilescheck(argvs):
    OS_CPU_N = os.cpu_count()
    #BUFF_SIZE = 0x30D4000  # 51Mb, 18.25s (33.46s) # Automatic cash - 26.7

    totflist = []
    #excludes = ('desktop.ini', 'thumbs.db')  # Files excluded from the process
    #includes = ('.wav', '.mp3', '.flac', '.wv')
    hashmap = {}  # content signature -> list of file names
    t0 = time.time()  # Start time marker
    path = r''.join(map(str, argvs[0:1]))
    path = r'D:\Music'
    # path = r'K:\alex\Test CDs'
    # path = r'Z:\Audio Music\FLACs\10CC'
    # path = r'Z:\Audio Music\FLACs\1988 Charlie Parker'
    # path = r'E:\Electric Light Orchestra - Discography'
    # path = r'N:\Audio Music\FLACs\10CC'
    print('Start time:', strftime('%H:%M:%S'))
    print('CPUs : ', OS_CPU_N)
    print(len(argvs), argvs[:], 'Path: ', path)


    async def read_file(path):
        try:
            async with aiofiles.open(path, 'rb') as f:
                cntn = await f.read()
            return cntn
        except Exception as e:
            print(e)
            #raise e

    def hash_calc():
        def hash_calc_in(fullname):
            print('File processing: ', fullname)
            h = hashlib.md5()
            # d = asyncio.run(read_file(fullname))
            # while d:
            # loop = asyncio.get_event_loop()
            # h.update(loop.run_until_complete(read_file(fullname)))
            h.update(asyncio.run(read_file(fullname)))
                #   d = asyncio.run(read_file(fullname))
            filelst = hashmap.setdefault(h.hexdigest(), [])
            filelst.append(fullname)
            print(' Done: ', fullname)
        with futures.ThreadPoolExecutor(max_workers=OS_CPU_N) as e:  # Start multi
            e.map(hash_calc_in, totflist)
            # for _ in fullname:
            #    e.submit(hash_calc_in, _)

    for path, dirs, files in os.walk(path):
        for filename in files:
            fullname_tmp = os.path.join(path, filename)
            if not os.path.getsize(fullname_tmp):  # zero-size files check skip (True=1 False=0)
                continue
            with open(fullname_tmp, 'r+b') as file:
                flinfo = filetype.guess(file)
            if str(flinfo).find('audio') == (-1):
                #  print('Skiped -> ', os.path.join(path, filename))
                continue
            if '\xb4' in fullname_tmp:
                fullname = fullname_tmp.replace('\xb4', '\x27')
                os.rename(fullname_tmp, fullname)
                print('File renamed: ' + fullname_tmp + ' ----> ' + fullname)
                totflist.append(fullname)
            else:
                totflist.append(fullname_tmp)

    #t = threading.Thread(target=hash_calc(totflist))
    #t.start()

    hash_calc()

    if not hashmap:
        exit('Path \''+path+'\' or file(s) not found.')

    print('Total files in the list for calculation: ', len(totflist))
    print('Records in the database file:', len(hashmap))
    pprint(list(hashmap.items()), depth=3, width=360)

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
        pprint(t['path'])


if __name__ == "__main__":
    audiofilescheck(sys.argv[1:])
