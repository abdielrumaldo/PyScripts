import random
import re
data = {"jsonfilekey": "MSI_JSON/03-1001_2_nolaepoc_SS_91.json", "data": {"Transform_PseudoColor": ["Transform_PseudoColor/Transform_PseudoColor_NOLA_SS_03-1001_91_Transform_PseudoColor_NOLA_SS_03-1001_91_PseudoColor_NOLA_SS_11_75_91_New_PseudoColor.tif"], "DataTransfer": "NewOrleans_ePOC_SS_03_23_2020_Masks", "createdDateTime": "2020-11-03 16:17:26", "Mask": "Mask/Mask_NOLA_SS_11_75_91_Mask_03-1001_Burn_Study75_Ant.L.U.Trunk_2_ImageColl_1_C2_Truth.png", "Study": 75, "Burn": "2", "Bucket": "nolaepoc", "Site": "NOLA", "SubjectID": "03-1001", "PseudoColor": "PseudoColor/PseudoColor_NOLA_SS_11_75_91_PseudoColor.png", "Raw": ["Raw/Raw_NOLA_SS_11_75_91.0_0_4_142118813309_2019-12-27_11.18.24.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_2_3_142119215576_2019-12-27_11.18.27.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_PseudoColor-raw.tif", "Raw/Raw_NOLA_SS_11_75_91.0_1_1_142118813316_2019-12-27_11.18.25.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_1_3_142118813316_2019-12-27_11.18.25.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_2_0_142119215576_2019-12-27_11.18.26.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_2_1_142119215576_2019-12-27_11.18.27.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_3_4_142119510261_2019-12-27_11.18.30.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_3_3_142119510261_2019-12-27_11.18.29.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_3_0_142119510261_2019-12-27_11.18.28.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_0_3_142118813309_2019-12-27_11.18.23.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_1_2_142118813316_2019-12-27_11.18.25.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_0_2_142118813309_2019-12-27_11.18.23.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_1_0_142118813316_2019-12-27_11.18.24.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_2_2_142119215576_2019-12-27_11.18.27.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_0_1_142118813309_2019-12-27_11.18.23.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_3_2_142119510261_2019-12-27_11.18.29.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_1_4_142118813316_2019-12-27_11.18.26.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_0_0_142118813309_2019-12-27_11.18.22.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_3_1_142119510261_2019-12-27_11.18.29.000.tiff", "Raw/Raw_NOLA_SS_11_75_91.0_2_4_142119215576_2019-12-27_11.18.28.000.tiff"], "DeviceType": "SS", "ReferenceImages": [], "Truth": "Truth/Truth_NOLA_SS_11_75_91.0_03-1001_Burn_Study75_Ant.L.U.Trunk_2_ImageColl_1_HP_Truth.tif", "Transform_Mask": ["Transform_Mask/Transform_Mask_NOLA_SS_03-1001_91_Transform_Mask_NOLA_SS_03-1001_91_Mask_NOLA_SS_11_75_91_Mask_03-1001_Burn_Study75_Ant.L.U.Trunk_2_ImageColl_1_C2_Truth.png"], "BurnIndex": 143, "AnatomicalLocation": "Ant.L.U.Trunk", "MSI": ["MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_0_000_F420_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_1_000_F525_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_2_000_F581_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_3_000_F620_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_4_000_F660_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_5_000_F725_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_6_000_F820_2019-12-27_11.18.22.000.tiff", "MSI/MSI_NOLA_SS_03-1001_91_MSI_NOLA_SS_11_75_91_7_000_F855_2019-12-27_11.18.22.000.tiff"], "ImageCollectionID": 91, "Assessing": ["Assessing/Assessing_NOLA_SS_11_75_91.0_03-1001_Burn_Study75_Ant.L.U.Trunk_2_ImageColl_1_JC_Assessing.tif"], "Transform_CJA": ["Transform_CJA/Transform_CJA_NOLA_SS_03-1001_91_New_Mask_03-1001_Burn_Study75_Ant.L.U.Trunk_2_ImageColl_1_JC_Assessing.png"], "Transform_Disp": ["Transform_Disp/Transform_Disp_NOLA_SS_03-1001_91_Disparity_2019-12-27_11.18.22.000.csv"]}}
def get_date_from(json_data):
    date = None
    while not date :
        file_names = json_data['data']['Raw']
        index = random.randrange(len(file_names))
        single_file = file_names[index]
        for key in single_file:
            print(key)
            date = re.search('[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]', key)
            if date:
                print(f'Found the date {date.group(0)} from the RAW image files names')
                return date.group(0)
            print("Iteration")
    return 0

data = {'rumaldo' : 1232}

value = list(data.values())[0]
print(value)

data_transfer_name = "AWCM_DFU_SMD2037-004-06-24-2021"
re_match = re.search('([^_]\w+?[_])', data_transfer_name).group(0)

print(re_match.strip('_').lower())