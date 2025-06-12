import subprocess
import pathlib
from tqdm.auto import tqdm


def cmd(cmdstr):
    return subprocess.run(cmdstr, shell=True, stdout=subprocess.PIPE, encoding='utf-8').stdout


def same_count(l, r):
    r = cmd(f'BaiduPCS-Go cd -l {r}')
    r.strip()
    r = r.split('\n')
    r = [tmp for tmp in r if '文件总数' in tmp]
    if not r:
        r = -1
    else:
        r = r[0]
        r = r.split()
        r = r[3]
        r = r[:-1]
        r = int(r)

    l = cmd(f'ls {l} | wc -l')
    l = int(l)

    return l, r


def main():
    local_root = pathlib.Path('/Volumes/T7/Templates/yunet')
    remote_root = pathlib.Path('/Templates/LRRM-U-TransNet')

    local_paths = []

    def has_subdir(local_path):
        for p in local_path.iterdir():
            if p.is_dir():
                return True
        return False

    for local_path in local_root.rglob('**/'):
        if not local_path.is_dir():
            continue
        if has_subdir(local_path):
            continue
        local_paths.append(local_path)

    for local_path in tqdm(local_paths):
        path = local_path.relative_to(local_root)
        remote_path = remote_root / path

        l, r = same_count(local_path, remote_path)
        if l == r:
            print(l, r, local_path, remote_path)


if __name__ == '__main__':
    main()
