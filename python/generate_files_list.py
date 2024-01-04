import os
import sys


def find_mgf_files(folder):

    mgf_list = []
    for root, _, files in os.walk(folder):
        if any(file.endswith('.mgf') for file in files):
            res = [root+os.sep+i for i in files]
            mgf_list.append(res)

    return mgf_list


def generate_res_folder(folder, output_base_dir):
    mgf_list = find_mgf_files(folder)
    for i in range(0, len(mgf_list)):
        file_list_str = '\n'.join(mgf_list[i])
        print("next: ")
        print(file_list_str)
        output_files = f"{output_base_dir}{os.sep}files_list_{i}.txt"
        with open(output_files,"w") as file:
            file.write(file_list_str)
    print("All task has completed, please check you folder!")

if __name__ == "__main__":
    
    folder = sys.argv[1]
    output_base_dir = sys.argv[2]

    # folder = r"C:\Users\ASUS\Desktop\generate_files_list_folder"
    # output_base_dir = r"C:\Users\ASUS\Desktop\generate_files_list_folder\files_list_dir"

    generate_res_folder(folder, output_base_dir)
    
