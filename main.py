import concurrent.futures
import json
import pathlib
import subprocess
from pathlib import Path

import pandas as pd
from rich.pretty import pprint
from tqdm.auto import tqdm


def cmd(cmdstr):
    return subprocess.run(cmdstr, shell=True, stdout=subprocess.PIPE, encoding='utf-8').stdout.strip()


def cd_and_ls(remote_path: str) -> list:
    res = cmd(
        f'BaiduPCS-Go cd -l \'{remote_path}\' | awk \'/^[[:space:]]*[0-9]+/ {{ for (i=5; i<=NF; i++) printf "%s%s", $i, (i==NF ? "" : " "); print "" }}\''
    )
    res = res.strip().split('\n')
    return res


def md5(remote_path):
    res = cmd(f"BaiduPCS-Go meta '{remote_path}' | grep md5 | awk '{{print $3}}'")
    res = res.strip()
    return res


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
        print(
            f'local_subpaths_len: {local_subpaths_len}, remote_subpaths_len: {remote_subpaths_len}, local_path: {local_path}, remote_path: {remote_path}'
        )

    remote_subpaths = remote_subpaths[3 : 3 + remote_subpaths_len]
    remote_subpaths = [pathlib.Path(r.strip().split()[-1]).as_posix() for r in remote_subpaths]

    for local_subpath in local_subpaths:
        if local_subpath not in remote_subpaths:
            return False, local_subpaths_len, remote_subpaths_len
    return True, local_subpaths_len, remote_subpaths_len


def run(local_path, remote_path, df):
    is_subset, local_subpaths_len, remote_subpaths_len = check_subset(local_path, remote_path)
    if not is_subset or local_subpaths_len != remote_subpaths_len:
        df.loc[f'{local_subpaths_len}--{remote_subpaths_len}--{local_path}--{remote_path}'] = [
            local_subpaths_len,
            remote_subpaths_len,
            local_path,
            remote_path,
        ]


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


def get_all_files(remote_path: str) -> list:
    """
    Recursively traverses a remote path to find all file paths.

    Args:
        remote_path: The starting remote path to search.

    Returns:
        A list of full paths to all files found.
    """
    all_files = []

    # Ensure the base path for constructing full paths is consistent
    base_path = remote_path.rstrip('/') + '/'

    # Get the contents of the current directory
    try:
        items = cd_and_ls(remote_path)
    except Exception as e:
        print(f'Error accessing path {remote_path}: {e}')
        return []

    for item in items:
        # Construct the full path of the current item
        full_item_path = base_path + item

        if item.endswith('/'):
            # This is a directory, so we recurse into it
            # and add the files found within to our list.
            subdirectory_files = get_all_files(full_item_path)
            all_files.extend(subdirectory_files)
        else:
            # This is a file, so we add its full path to the list.
            all_files.append(f'{full_item_path}++{md5(full_item_path)}')

    return all_files


def main1():
    # Define a dictionary of tasks: {output_filename: input_path}
    tasks_to_run = {
        'res1.json': '/苗振new/苗振等多个文件/苗振',
        'res2.json': '/苗振new/苗振等多个文件/010.苗振合集',
        # You can easily add more tasks here!
        # 'res3.json': '/another/path/to/scan',
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Create a dictionary mapping each future to its output filename
        future_to_filename = {executor.submit(get_all_files, path): filename for filename, path in tasks_to_run.items()}

        # Use as_completed to process results as they finish
        for future in concurrent.futures.as_completed(future_to_filename):
            filename = future_to_filename[future]
            try:
                # Get the result from the completed future
                result_data = future.result()

                print(f"Task for '{filename}' completed successfully.")

                # Write the result to the corresponding file
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(result_data, f, ensure_ascii=False, indent=4)

            except Exception as exc:
                print(f"Task for '{filename}' generated an exception: {exc}")


def main2():
    with open('res1.json', 'r', encoding='utf-8') as f:
        res1 = json.load(f)
    with open('res2.json', 'r', encoding='utf-8') as f:
        res2 = json.load(f)
    # tmp1 = set([r.split('++')[1] for r in res1])
    # tmp2 = set([r.split('++')[1] for r in res2])
    # print(tmp1)
    # print(tmp2)
    # print(len(tmp1) == len(list(set(tmp1))))
    # print(len(tmp2) == len(list(set(tmp2))))
    res1 = {r.split('++')[1]: r.split('++')[0] for r in res1}
    res2 = {r.split('++')[1]: r.split('++')[0] for r in res2}

    common_md5 = set(res1.keys()) & set(res2.keys())
    print(f'Common MD5 count: {len(common_md5)}')
    common_files = [(md5, res1[md5], res2[md5]) for md5 in common_md5]
    with open('common_files.json', 'w', encoding='utf-8') as f:
        json.dump(common_files, f, ensure_ascii=False, indent=4)


def find_common_base_paths(path1_str, path2_str):
    """
    找到两个路径中最后一个相同的文件夹名，并返回从这个文件夹开始到路径开头的部分

    Args:
        path1_str: 第一个路径字符串
        path2_str: 第二个路径字符串

    Returns:
        tuple: 处理后的两个路径
    """
    path1 = Path(path1_str)
    path2 = Path(path2_str)

    # 获取路径的所有部分
    parts1 = path1.parts
    parts2 = path2.parts

    # 从后往前找到最后一个相同的位置
    common_index = -1
    for i in range(min(len(parts1), len(parts2))):
        idx = -i - 1  # 从后往前索引
        if parts1[idx] != parts2[idx]:
            break
        common_index = idx

    # 如果没有相同的部分，返回空路径
    if common_index == -1:
        return Path(), Path()

    # 构建返回路径（从开头到最后一个相同部分）
    result1 = Path(*parts1[: common_index + 1]) if common_index >= -len(parts1) else Path()
    result2 = Path(*parts2[: common_index + 1]) if common_index >= -len(parts2) else Path()

    return result1, result2


def main3():
    with open('common_files.json', 'r', encoding='utf-8') as f:
        common_files = json.load(f)

    common_bases = {}

    for common_file in common_files:
        common_base1, common_base2 = find_common_base_paths(common_file[1], common_file[2])
        common_bases[common_base1.as_posix()] = common_base2.as_posix()
    common_bases = [(k, v) for k, v in common_bases.items()]
    with open('common_bases.json', 'w', encoding='utf-8') as f:
        json.dump(common_bases, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    main3()
