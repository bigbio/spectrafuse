import pyarrow.parquet as pq
import pandas as pd
import numpy as np
import re
from pathlib import Path
from collections import defaultdict
import sys
from constant import UseCol


class ParquetPathHandler:
    parquet_path = ""
    path_obj = None

    def __init__(self, path):
        self.parquet_path = path
        self.path_obj = Path(path)

    def get_mgf_filename(self, mgf_file_index: int = 1, ) -> str:
        filename = f"{self.path_obj.parts[-1].split('.')[0]}_{str(mgf_file_index)}.mgf"

        return filename

    def get_item_info(self) -> str:
        return self.path_obj.parts[-1].split('-')[0]


def read_sdrf(sdrf_folder: str) -> dict:
    """
    Read the sdrf file to obtain the relationship between samples and instruments and species in a project
    :param sdrf_folder: sdrf folder
    :return:
    """
    sdrf_df = pd.read_csv(sdrf_folder, sep='\t')
    sdrf_feature_df = pd.DataFrame()
    try:
        sdrf_feature_df = sdrf_df.loc[:, ['comment[data file]', 'characteristics[organism]', 'comment[instrument]']]
    except KeyError:
        print(f'{sdrf_folder} file has some format error, please check the col index format.')

    sdrf_feature_df['comment[data file]'] = sdrf_feature_df['comment[data file]'].apply(lambda x: x.split('.')[0])
    sdrf_feature_df['comment[instrument]'] = sdrf_feature_df[
        'comment[instrument]'].apply(lambda x: re.search(r'=(.*)', x.split(';')[0]).group(1))
    sdrf_feature_df['organism_instrument'] = sdrf_feature_df[
        ['characteristics[organism]', 'comment[instrument]']].apply(lambda x: list(x), axis=1)
    sample_info_dict = sdrf_feature_df.set_index('comment[data file]')['organism_instrument'].to_dict()
    return sample_info_dict


def get_spectrum(row, dataset_id: str):
    res_str = (f"BEGIN IONS\n"  # begin
               f'TITLE=id=mzspec:{dataset_id}:'
               f'{row["reference_file_name"]}:'
               f'scan:{str(row["scan_number"])}:{row["sequence"]}/{row["charge"]}\n'  # usi
               f'PEPMASS={str(row["exp_mass_to_charge"])}\n'  # pepmass
               f'CHARGE={str(row["charge"])}+\n'  # charge
               f'{get_mz_intensity_str(row["mz_array"], row["intensity_array"])}\n'  # mz and intensity
               f'END IONS'  # end
               )

    return res_str


def get_mz_intensity_str(mz_series, intensity_series) -> str:
    """
    Combine the m/z and intensity arrays into a single string
    :param mz_series: m/z array
    :param intensity_series: intesity array
    :return:
    """
    combined_str = ""

    if mz_series is not None and intensity_series is not None:
        mz_array_np = np.array(mz_series)
        intensity_array_np = np.array(intensity_series)
        # Convert each element of the NumPy array to a string and concatenate it
        combined_np = np.core.defchararray.add(
            np.char.add(mz_array_np.astype(str), ' '),
            intensity_array_np.astype(str)
        )
        combined_str = '\n'.join(combined_np)

    return combined_str


def iter_parquet_dir(dir_path: str) -> list:
    """
    Extract the path information for all parquet files from the parquet Files subdirectory and return a list
    :param dir_path: parquet file's path
    :return:
    """
    parquet_path_lst = []
    directory_path = Path(dir_path)
    parquet_files = directory_path.rglob('*.parquet')

    # Iterate over all matching.parquet files
    for parquet_file in parquet_files:
        if parquet_file.parts[-2] == 'parquet_files':
            parquet_path_lst.append(parquet_file)
    return parquet_path_lst


def convert_to_mgf(parquet_path: str, sdrf_path: str, output_path: str, batch_size, spectra_capacity) -> None:
    """
     A single parquet file is read in blocks, and then grouped by species, instrument, charge,
     and converted to parquet files
    :param parquet_path: The full path to the parquet file
    :param sdrf_path:The sdrf metadata file for the project
    :param output_path: output dir
    :param batch_size: default size is 60000
    :param spectra_capacity: default size is 1000000
    :return:
    """
    parquet_file_path = parquet_path
    sdrf_file_path = sdrf_path
    res_file_path = output_path

    basename = ParquetPathHandler(parquet_path).get_item_info()

    # loading Parquet file
    parquet_file = pq.ParquetFile(parquet_path)
    write_count_dict = defaultdict(int)  # Counting dictionary
    relation_dict = defaultdict(int)  # the file index dictionary
    SPECTRA_NUM = spectra_capacity  # The spectra capacity of one mgf
    BATCH_SIZE = batch_size  # The batch size of each parquet pass

    for parquet_batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=UseCol.PARQUET_COL_TO_MGF.value):
        mgf_group_df = pd.DataFrame()
        row_group = parquet_batch.to_pandas()

        # spectrum
        mgf_group_df['spectrum'] = row_group.apply(lambda row: get_spectrum(row, basename), axis=1)

        sample_info_dict = read_sdrf(sdrf_file_path)

        mgf_group_df['mgf_file_path'] = row_group.apply(
            lambda row: '/'.join(sample_info_dict.get(row['reference_file_name']) +
                                 ['charge' + str(row["charge"]), 'mgf files']), axis=1)

        for group, group_df in mgf_group_df.groupby('mgf_file_path'):
            # 为当前group创建或打开文件
            base_mgf_path = f"{res_file_path}/{group}"
            mgf_file_path = (f"{base_mgf_path}/{Path(parquet_file_path).parts[-1].split('.')[0]}_"
                             f"{relation_dict[base_mgf_path] + 1}.mgf")
            Path(mgf_file_path).parent.mkdir(parents=True, exist_ok=True)

            if write_count_dict[group] + group_df.shape[0] <= SPECTRA_NUM:
                with open(mgf_file_path, 'a') as f:
                    f.write('\n'.join(group_df["spectrum"]))

                # 更新计数字典
                write_count_dict[group] += group_df.shape[0]
            else:
                # 剩余了多少容量
                remain_num = SPECTRA_NUM - write_count_dict[group]

                # 重置该group对应的计数
                # remain的部分加入到原来的部分
                with open(mgf_file_path, 'a') as f:
                    f.write('\n'.join(group_df.head(remain_num)["spectrum"]))

                # 文件索引＋1
                relation_dict[base_mgf_path] += 1
                # 重置次数
                write_count_dict[group] = 0
                # 更新mgf文件地址
                mgf_file_path = (f"{base_mgf_path}/{Path(parquet_file_path).parts[-1].split('.')[0]}_"
                                 f"{relation_dict[base_mgf_path] + 1}.mgf")
                with open(mgf_file_path, 'a') as f:
                    f.write('\n'.join(group_df.tail(group_df.shape[0] - remain_num)["spectrum"]))


def generate_mgf_files(parquet_dir: str, sdrf_file_path: str, output_path: str,
                       batch_size: int = 1000000, spectra_capacity: int = 1000000) -> None:
    parquet_file_path_lst = iter_parquet_dir(parquet_dir)

    for parquet_file_path in parquet_file_path_lst:
        print(f"Converting {Path(parquet_file_path).parts[-1]} files to MGF format...")
        convert_to_mgf(parquet_file_path, sdrf_file_path, output_path,
                       batch_size=batch_size, spectra_capacity=spectra_capacity)

    print(f"All tasks have completed...")


if __name__ == '__main__':
    parquet_dir = sys.argv[1]
    sdrf_file_path = sys.argv[2]
    res_file_path = sys.argv[3]

    # parquet_dir = r'G:\graduation_project\generate-spectrum-library\PXD002179'
    # sdrf_file_path = r'G:\graduation_project\generate-spectrum-library\PXD002179\mztab\PXD002179.sdrf.tsv'
    # res_file_path = r'G:\graduation_project\generate-spectrum-library\test\PXD002179_copy'

    generate_mgf_files(parquet_dir=parquet_dir, sdrf_file_path=sdrf_file_path, output_path=res_file_path)
