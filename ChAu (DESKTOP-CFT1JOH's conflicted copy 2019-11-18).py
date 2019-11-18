"""
    Main func is 'audio_files_check'
"""
import hashlib
import os
import sys
import time
from concurrent import futures
from pprint import pprint
from time import strftime
import dataset
import filetype


def audio_files_check(argvs):
    """
        Read all files from dir (path as argument),
        select only music files from (not by extension, but content signature)
        calculate MD5 for everyone,
        check and mark duplicated,
        put all of them into database (Allfiles.db) and txt (Allfiles.lst) files,
    """
    os_cpu_n = os.cpu_count()
    totflist = []
    hashmap = {}                             # list of audio files were found
    t0 = time.time()                         # Program time start marker
    mypath = r''.join(map(str, argvs[0:1]))  # Path for search (argument)
    mypath = r'D:\Music'
    # mypath = r'D:\Downloads\Dropbox\Python'
    print('Program start time:', strftime('%H:%M:%S'))
    print('CPUs : ', os_cpu_n)
    print('Path : ', mypath)

    def ttm(t):
        msg = '{:.2f} s'
        print('End time:', strftime('%H:%M:%S'), ' ( The program has taken ', msg.format(time.time()-t), ')')

    def read_allfiles():
        """
        Read all files, select only audio
        """
        for path, dirs, files in os.walk(mypath):
            for filename in files:
                fullname_tmp = os.path.join(path, filename)
                if not os.path.getsize(fullname_tmp):  # zero-size files check skip (True=1 False=0)
                    continue
                with open(fullname_tmp, 'rb') as file:
                    fl_info = filetype.guess(file)
                if str(fl_info).find('audio') == (-1):
                    continue
                if '\xb4' in fullname_tmp:
                    fullname = fullname_tmp.replace('\xb4', '\x27')
                    os.rename(fullname_tmp, fullname)
                    print('File renamed: ' + fullname_tmp + ' ----> ' + fullname)
                    totflist.append(fullname)
                else:
                    totflist.append(fullname_tmp)
        print('Total files in a que for calculation:', len(totflist))

    def hash_calc():
        """
        Calculate MD5 for selected files
        """
        def hash_calc_in(fullname):
            """
            Calculate one file in one thread
            """
            print('File processing: ', fullname)
            h = hashlib.md5()
            with open(fullname, 'rb') as f:
                h.update(f.read())
            filelst = hashmap.setdefault(h.hexdigest(), [])
            filelst.append(fullname)
            print(' Done: ', fullname)

        with futures.ThreadPoolExecutor() as e:  # Start multithread process
            e.map(hash_calc_in, totflist)

#  ------------- START -------------
    read_allfiles()
    hash_calc()

    if not hashmap:
        exit('Path \'' + mypath + '\' or any appropriate file(s) in the path not found.')

    pprint(list(hashmap.items()), depth=3, width=360)

    with open('AllFiles.lst', 'w') as f:  # write list of the files to the one txt(lst)-file
        for m in hashmap.keys():
            print(str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), file=f)

    print(strftime('%H:%M:%S'), ' - Start DB maintenance process... ')
    t1 = time.time()

    with dataset.connect('sqlite:///Allfiles.db') as tx:
        for m in hashmap.keys():  # Update database
            key = 1 if "', '" in str(hashmap[m]) else 0
            tx['Files'].upsert(dict(md5=m, path=str(hashmap[m]).strip('\'["]').replace('\\\\', '\\'), dups=key),['md5'])
        for m in tx['Files']:                               # Cleanup database
            if m['md5'] not in hashmap.keys():
                tx['Files'].delete()
                print('- record deleted: ', m['path'])
        for t in tx['Files'].find(dups=True):
            print('Duplicated record(s) found:')
            print('\n'.join(str(t['path']).replace('\'', '').split(', ')), '--->', t['md5'])

    ttm(t1)
    ttm(t0)


if __name__ == "__main__":
    audio_files_check(sys.argv[1:])
