import os
import pyarrow.parquet as pq
import sys
import pandas as pd


def read_sdrf(sdrf_folder: str) -> dict:
    sdrf_df = pd.read_csv(sdrf_folder, sep='\t')
    sdrf_feature_df = pd.DataFrame()
    try:
        sdrf_feature_df = sdrf_df.loc[:, ['comment[data file]', 'characteristics[organism]', 'comment[instrument]']]
    except KeyError:
        print(f'{sdrf_folder} file has some format error, please check the col index format.')

    sdrf_feature_df['comment[data file]'] = sdrf_feature_df['comment[data file]'].apply(lambda x: x.split('.')[0])
    sdrf_feature_df['comment[instrument]'] = sdrf_feature_df[
        'comment[instrument]'].apply(lambda x: x.split(';')[0][4:])
    sdrf_feature_df['organism_instrument'] = sdrf_feature_df[
        ['characteristics[organism]', 'comment[instrument]']].apply(lambda x: list(x), axis=1)
    sample_info_dict = sdrf_feature_df.set_index('comment[data file]')['organism_instrument'].to_dict()
    return sample_info_dict


def mk_mgf(mgf_file_path: str, spectrum_str: str) -> None:
    dirname = os.path.dirname(mgf_file_path)
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    with open(mgf_file_path, 'a') as f:
        f.write(spectrum_str)


def mgf_converter(project_folder: str, sdrf_folder: str, output_folder: str) -> None:
    os.chdir(project_folder)
    sample_info_dict = read_sdrf(sdrf_folder)
    # basename = os.path.basename(project_folder)
    basename = os.path.basename(sdrf_folder).split('.')[0]
    # current_file = None
    # current_file_path = None

    for folder in os.listdir():
        if folder.startswith('parquet'):
            for parquet_file in os.listdir(folder):
                parquet_file_path = os.path.join(folder, parquet_file)
                print(parquet_file_path)
                parquet_file_obj = pq.ParquetFile(parquet_file_path)

                # for row_group
                for i in range(parquet_file_obj.num_row_groups):
                    row_group = parquet_file_obj.read_row_group(i)
                    row_group = row_group.to_pandas()
                    write_count = 0  # record write count
                    file_index = 0  # record file index
                    # for one row
                    for _, row in row_group.iterrows():
                        output_lines = (
                            f'BEGIN IONS\n'
                            f'TITLE=id=mzspec:{basename}:'
                            f'{row["reference_file_name"]}:scan:{str(row["scan_number"])},'
                            f'sequence:{row["peptidoform"]},'
                            f'q_value:{str(row["protein_global_qvalue"])}\n'
                            f'PEPMASS={str(row["exp_mass_to_charge"])}\n'
                            f'CHARGE={str(row["charge"])}+\n'
                        )
                        mz_intensity_str = ""
                        for mz, intensity in zip(row['mz_array'], row['intensity_array']):
                            mz_intensity_str += f'{str(mz)} {str(intensity)}\n'

                        output_lines = f'{output_lines}{mz_intensity_str}END IONS\n'
                        print(output_lines)
                        base_folder = '/'.join(sample_info_dict.get(row['reference_file_name']))

                        if write_count % 1000000 == 0: # if the spectrum's num over 1000000, the file index+1
                            file_index += 1

                        mgf_folder = (f'{output_folder}/{base_folder}/charge{str(row["charge"])}/mgf_files/'
                                      f'{basename}-charge{str(row["charge"])}_{file_index}.mgf')
                        mk_mgf(mgf_folder, output_lines)

                        write_count += 1


if __name__ == '__main__':
    # pxd_folder = 'G:\\graduation_project\\generate-spectrum-library\\PXD008934\\mztab'
    # output_folder = 'G:\\graduation_project\\generate-spectrum-library\\project_folder'
    # sdrf_file = pxd_folder + '\\PXD008934.sdrf.tsv'
    
    pxd_folder = sys.argv[1]
    sdrf_file = sys.argv[2]
    output_folder = sys.argv[3]
    mgf_converter(pxd_folder, sdrf_file, output_folder)
