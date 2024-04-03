from pathlib import Path
import pandas as pd
import numpy as np
from constant import UseCol


def resolve_fileLoc(root_directory: str, file_pattern: str) -> Path:
    """
    在指定目录中返回指定文件名字的完整路径Path对象
    :param root_directory: 目录
    :param file_pattern: 文明名字
    :return:
    """
    root_directory = Path(root_directory)
    # 递归搜索文件
    file_path = root_directory.rglob(file_pattern)

    # 找到第一个匹配的文件并获取其绝对路径
    try:
        found_file = next(file_path)
        absolute_path = found_file.resolve()
        return absolute_path
    except StopIteration:
        print(f"No file matching '{file_pattern}' found in the directory tree.")
    return Path()


def extract_mgf_path_info(usi_str: str) -> str:
    """
    提取mgf_path信息，以聚类结果文件为标准
    :param usi_str:
    :return:
    """

    path = Path(usi_str)
    specie_str = path.parts[-5]
    instrument_str = path.parts[-4]

    return f"{specie_str},{instrument_str}"


def extract_cluster_tsv(cluster_res_path: str, pxd_folder: str):
    """
    复合簇和独立簇要分开提取信息吗，检索然后删除复合簇中大于10个的psm才会进行判断
    :param pxd_folder: quantmsio得到的目录
    :param cluster_res_path:聚类结果路径
    :return:
    """
    groups_list = []
    columns_to_filter = UseCol.PARQUET_COL_TO_FILTER.value
    specie_instrument = ['species', 'instrument']
    usi_list = UseCol.PARQUET_COL_TO_USI.value
    parquet_col = UseCol.PARQUET_COL_TO_MGF.value + columns_to_filter

    cluster_res_df = pd.read_csv(cluster_res_path, sep='\t', header=None)
    cluster_res_df.columns = ['mgf_path', 'index', 'cluster_accession']
    cluster_res_df.dropna(axis=0, inplace=True)  # 删除空行

    # 提取出mgf文件的名字,根据这个去找对应的parquet文件
    cluster_res_df['parquet_path'] = (
        cluster_res_df['mgf_path'].apply(
            lambda x: resolve_fileLoc(pxd_folder, Path(x).parts[-1].split('_')[0] + ".parquet")))

    for path_path_str, group in cluster_res_df.groupby('parquet_path'):
        group_parquet = pd.read_parquet(path_path_str, columns=parquet_col)
        group_index = group['index'].to_list()

        # 从quantmsio提取'posterior_error_probability', 'global_qvalue'
        group[columns_to_filter] = group_parquet.loc[group_index, columns_to_filter].reset_index(drop=True)

        # 物种和仪器
        group[specie_instrument] = (group['mgf_path'].apply(lambda x: extract_mgf_path_info(x))
                                    .str.split(',', expand=True))

        # usi
        group['usi'] = (group_parquet.loc[group_index, usi_list].
                        apply(lambda row: f"mzspec:{Path(path_path_str).parts[-1].split('-')[0]}:"
                                          f"{row['reference_file_name']}:"
                                          f"scan:{row['scan_number']}:"
                                          f"sequence:{row['sequence']}/{row['charge']}", axis=1))
        group[['mz_array', 'intensity_array']] = group_parquet.loc[group_index, ['mz_array', 'intensity_array']]

        groups_list.append(group)

    group_stack = np.vstack([group.values for group in groups_list])
    use_cols = (cluster_res_df.columns.to_list() + columns_to_filter +
                specie_instrument + ['usi', 'mz_array', 'intensity_array'])

    res_table = pd.DataFrame(group_stack, columns=use_cols)

    return res_table


if __name__ == '__main__':
    #  mgf文件
    mgf_path = (r'G:\graduation_project\generate-spectrum-library\project_folder'
                r'\Homo sapiens\TQ Orbitrap XL\charge5\mgf_files\PXD002179-charge5_1.mgf')
    cluster_tsv_path = (r'G:\graduation_project\generate-spectrum-library\project_folder\res_output_folder'
                        r'\files_list_3\maracluster_output\MaRaCluster.clusters_p30.tsv')
    pxd_folder_path = r'G:\graduation_project\generate-spectrum-library\PXD002179\mztab'

    # MaRaCluster结果文件, 默认选取_p30.tsv
    res_df = extract_cluster_tsv(cluster_tsv_path, pxd_folder_path)
    print(res_df.head())
    # res_df.to_csv(r"C:\Users\ASUS\Desktop\test_parquet.csv", index=False)
    # print(extract_mgf(mgf_path))
