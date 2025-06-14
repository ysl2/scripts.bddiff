import concurrent.futures
import pathlib
import subprocess

import pandas as pd
from tqdm.auto import tqdm


def cmd(cmdstr):
    return subprocess.run(cmdstr, shell=True, stdout=subprocess.PIPE, encoding='utf-8').stdout.strip()


def check_subset(local_path, remote_path):
    local_subpaths = cmd(f'ls {local_path}')
    local_subpaths = local_subpaths.split()
    local_subpaths_len = len(local_subpaths)

    remote_subpaths = cmd(f'BaiduPCS-Go ls {remote_path}')
    remote_subpaths = remote_subpaths.split('\n')

    remote_subpaths_len = [r for r in remote_subpaths if '文件总数' in r or '目录总数' in r]
    remote_subpaths_len = remote_subpaths_len[0]
    remote_subpaths_files_len = int(remote_subpaths_len.split('文件总数: ')[1].split()[0][:-1])
    remote_subpaths_folders_len = int(remote_subpaths_len.split('目录总数: ')[1].strip())
    remote_subpaths_len = remote_subpaths_files_len + remote_subpaths_folders_len
    if local_subpaths_len != remote_subpaths_len:
        print(f'local_subpaths_len: {local_subpaths_len}, remote_subpaths_len: {remote_subpaths_len}, local_path: {local_path}, remote_path: {remote_path}')

    remote_subpaths = remote_subpaths[3 : 3 + remote_subpaths_len]
    remote_subpaths = [pathlib.Path(r.strip().split()[-1]).as_posix() for r in remote_subpaths]

    for local_subpath in local_subpaths:
        if local_subpath not in remote_subpaths:
            return False, local_subpaths_len, remote_subpaths_len
    return True, local_subpaths_len, remote_subpaths_len


def run(local_path, remote_path, df):
    is_subset, local_subpaths_len, remote_subpaths_len = check_subset(local_path, remote_path)
    if not is_subset or local_subpaths_len != remote_subpaths_len:
        df.loc[f'{local_subpaths_len}--{remote_subpaths_len}--{local_path}--{remote_path}'] = [local_subpaths_len, remote_subpaths_len, local_path, remote_path]


def main():
    local_root = pathlib.Path('/Volumes/T7/Templates/yunet')
    remote_root = pathlib.Path('/Templates/LRRM-U-TransNet')

    local_paths = []

    # def has_subdir(local_path):
    #     for p in local_path.iterdir():
    #         if p.is_dir():
    #             return True
    #     return False

    for local_path in local_root.rglob('**/'):
        if not local_path.is_dir():
            continue
        # if has_subdir(local_path):
        #     continue
        local_paths.append(local_path)

    df = pd.DataFrame(columns=['local_subpaths_len', 'remote_subpaths_len', 'local_path', 'remote_path'])
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        for local_path in local_paths:
            path = local_path.relative_to(local_root)
            remote_path = remote_root / path

            local_path = local_path.as_posix()
            remote_path = remote_path.as_posix()

            futures.append(executor.submit(run, local_path, remote_path, df))

        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
            pass

    df.to_csv('result.csv', index=False)


if __name__ == '__main__':
    main()
