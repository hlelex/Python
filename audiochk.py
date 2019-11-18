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
#import fleep
import filetype


def isMp3Valid(file_path):
    """
    Thank you for the code. I could use it for my work.
    but it would be better if you change
    * open(file_path, ‘r’) -> open(file_path, ‘rb’)
    * padding = ord(frame_hdr[2]) & 1 == 1 -> padding = ord(frame_hdr[2]) & 2
    """
    is_valid = False

    f = open(file_path, 'r')
    block = f.read(1024)
    frame_start = block.find(chr(255))
    block_count = 0  # abort after 64k
    while len(block) > 0 and frame_start == -1 and block_count < 64:
        block = f.read(1024)
        frame_start = block.find(chr(255))
        block_count += 1

    if frame_start > -1:
        frame_hdr = block[frame_start:frame_start + 4]
        is_valid = frame_hdr[0] == chr(255)

        mpeg_version = ''
        layer_desc = ''
        uses_crc = False
        bitrate = 0
        sample_rate = 0
        padding = False
        frame_length = 0

        if is_valid:
            is_valid = ord(frame_hdr[1]) & 0xe0 == 0xe0  # validate the rest of the frame_sync bits exist

        if is_valid:
            if ord(frame_hdr[1]) & 0x18 == 0:
                mpeg_version = '2.5'
            elif ord(frame_hdr[1]) & 0x18 == 0x10:
                mpeg_version = '2'
            elif ord(frame_hdr[1]) & 0x18 == 0x18:
                mpeg_version = '1'
            else:
                is_valid = False

        if is_valid:
            if ord(frame_hdr[1]) & 6 == 2:
                layer_desc = 'Layer III'
            elif ord(frame_hdr[1]) & 6 == 4:
                layer_desc = 'Layer II'
            elif ord(frame_hdr[1]) & 6 == 6:
                layer_desc = 'Layer I'
            else:
                is_valid = False

        if is_valid:
            uses_crc = ord(frame_hdr[1]) & 1 == 0

            bitrate_chart = [
                [0, 0, 0, 0, 0],
                [32, 32, 32, 32, 8],
                [64, 48, 40, 48, 16],
                [96, 56, 48, 56, 24],
                [128, 64, 56, 64, 32],
                [160, 80, 64, 80, 40],
                [192, 96, 80, 96, 40],
                [224, 112, 96, 112, 56],
                [256, 128, 112, 128, 64],
                [288, 160, 128, 144, 80],
                [320, 192, 160, 160, 96],
                [352, 224, 192, 176, 112],
                [384, 256, 224, 192, 128],
                [416, 320, 256, 224, 144],
                [448, 384, 320, 256, 160]]
            bitrate_index = ord(frame_hdr[2]) >> 4
            if bitrate_index == 15:
                is_valid = False
            else:
                bitrate_col = 0
                if mpeg_version == '1':
                    if layer_desc == 'Layer I':
                        bitrate_col = 0
                    elif layer_desc == 'Layer II':
                        bitrate_col = 1
                    else:
                        bitrate_col = 2
                else:
                    if layer_desc == 'Layer I':
                        bitrate_col = 3
                    else:
                        bitrate_col = 4
                bitrate = bitrate_chart[bitrate_index][bitrate_col]
                is_valid = bitrate > 0

        if is_valid:
            sample_rate_chart = [
                [44100, 22050, 11025],
                [48000, 24000, 12000],
                [32000, 16000, 8000]]
            sample_rate_index = (ord(frame_hdr[2]) & 0xc) >> 2
            if sample_rate_index != 3:
                sample_rate_col = 0
                if mpeg_version == '1':
                    sample_rate_col = 0
                elif mpeg_version == '2':
                    sample_rate_col = 1
                else:
                    sample_rate_col = 2
                sample_rate = sample_rate_chart[sample_rate_index][sample_rate_col]
            else:
                is_valid = False

        if is_valid:
            padding = ord(frame_hdr[2]) & 1 == 1

            padding_length = 0
            if layer_desc == 'Layer I':
                if padding:
                    padding_length = 4
                frame_length = (12 * bitrate * 1000 / sample_rate + padding_length) * 4
            else:
                if padding:
                    padding_length = 1
                frame_length = 144 * bitrate * 1000 / sample_rate + padding_length
            is_valid = frame_length > 0

            # Verify the next frame
            if (frame_start + frame_length < len(block)):
                is_valid = block[frame_start + frame_length] == chr(255)
            else:
                offset = (frame_start + frame_length) - len(block)
                block = f.read(1024)
                if len(block) > offset:
                    is_valid = block[offset] == chr(255)
                else:
                    is_valid = False

    f.close()
    return is_valid


def audiofilescheck(argvs):
    OS_CPU_N = os.cpu_count()

    totflist = []
    hashmap = {}        # content signature -> list of file names
    t0 = time.time()    # Start time marker
    path = r''.join(map(str, argvs[0:1]))

    # path = r'E:\From disk Data\From Old PC\Documents and Settings\alexa\Desktop'
    path = r'D:\Music'
    # path = r'K:\alex\Test CDs'
    # path = r'Z:\Audio Music\FLACs\10CC'
    # path = r'Z:\Audio Music\FLACs\1988 Charlie Parker'
    # path = r'E:\Electric Light Orchestra - Discography'

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

    def hash_calc():
        def hash_calc_in (fullname):
            print('File processing: ', fullname)
            h = hashlib.md5()
            h.update(asyncio.run(read_file(fullname)))
            filelst = hashmap.setdefault(h.hexdigest(), [])
            filelst.append(fullname)
            print(' Done: ', fullname)
        with futures.ThreadPoolExecutor(max_workers=OS_CPU_N) as e:  # Start multi
            e.map(hash_calc_in, totflist)

    for path, dirs, files in os.walk(path):
        for filename in files:
            fullname_tmp = os.path.join(path, filename)
            if not os.path.getsize(fullname_tmp):  # zero-size files check skip (True=1 False=0)
                continue
            with open(fullname_tmp, 'r+b') as file:
                flinfo = filetype.guess(file)
            if str(flinfo).find('audio') == (-1):
                #print('Skiped -> ', os.path.join(path, filename))
                continue
            if '\xb4' in fullname_tmp:
                fullname = fullname_tmp.replace('\xb4', '\x27')
                os.rename(fullname_tmp, fullname)
                print('File renamed: ' + fullname_tmp + ' ----> ' + fullname)
                totflist.append(fullname)
            else:
                totflist.append(fullname_tmp)

    hash_calc()

    if not hashmap:
        exit('Path \''+path+'\' or any appropriate file(s) not found.')

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
