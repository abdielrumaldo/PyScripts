import numpy as np
import boto3
from PIL import Image
from io import BytesIO
import cv2 as cv2
import matplotlib.pyplot as plt
import json
import random
import re


# define plotting colormap
from matplotlib.colors import ListedColormap
label = [0,1,2,3]
colors = ['red','green','blue','purple']

# define class labels
label_dict = {'first': [44, 160, 44],
              'third': [214, 39, 40], 
              'superficial_second': [31, 119, 180], 
              'deep_second': [255, 127, 14],
              'background': [0,0,0], 
              'healthy': [148, 103, 189]}

# define expansion move rotation order
rotation_order = ['third', 'deep_second', 'superficial_second', 'first', 'healthy', 'background']


#prob_fg = {'white':[255,255,255]}

### Function Set ###

def create_initial_masks(GT_image_A, GT_image_B, label, label_RGB_dict = label_dict,verbose=False):
    '''
    mask labels for Grab Cut algorithm:
        0 = definite background
        1 = definite foreground
        2 = probable background
        3 = probable foreground
    
    '''
    
    if GT_image_A.shape != GT_image_B.shape:
        print('dimensions of GT_image_A, {}, do not equal dimensions of GT_image_B, {}'.format(GT_image_A.shape, GT_image_B.shape))
        return
    
    # get image dims
    H,W,C = GT_image_A.shape[0], GT_image_A.shape[1], GT_image_A.shape[2]
    
    # convert image to vector in R3
    mask_vect_A = GT_image_A.reshape(H*W, C)
    mask_vect_B = GT_image_B.reshape(H*W, C)
    
    mask_A_Foreground = np.where(mask_vect_A == label_RGB_dict[label], 1, 0) # 1 if True, 0 if False
    mask_B_Foreground = np.where(mask_vect_B == label_RGB_dict[label], 1, 0) # 1 if True, 0 if False
    
    # find all elements of the mask vector that have RGB values equal to the label RGB value and store them as a bianry vector
    new_mask__Foreground = np.where((mask_vect_A == label_RGB_dict[label])&(mask_vect_B == label_RGB_dict[label]), 2, 0) # 1 if True, 0 if False
    
    new_mask__Potential_Foreground = np.where((mask_vect_A == label_RGB_dict[label])|(mask_vect_B == label_RGB_dict[label]), 3, 0) # 1 if True, 0 if False

    new_mask = new_mask__Potential_Foreground - new_mask__Foreground

    # reshape the bianry mask vector into image of same dimensions as origianl mask but only one chanel
    new_mask = new_mask[:,0].reshape(H,W)
    
#    # view the white as the probable forground: 3
#    AB_overlay = np.where((mask_vect_A == label_RGB_dict[label])|(mask_vect_B == label_RGB_dict[label]), 0, 3) # 1 if True, 0 if False
#    AB_overlay = AB_overlay[:,0].reshape(H,W)
#    new_mask = new_mask +AB_overlay
    
    if verbose==True:
        plt.imshow(new_mask, cmap=ListedColormap(colors))
        plt.title("Intial Mask for " + label)
        plt.tight_layout()
        plt.show()
    
    return new_mask.astype('uint8')

def perform_grab_cut(color_image, label_mask, verbose = False):
    
    mask = np.copy(label_mask)
    
    bgdModel = np.zeros((1,65),np.float64)
    fgdModel = np.zeros((1,65),np.float64)
    
    mask, bgdModel, fgdModel = cv2.grabCut(color_image, mask, None, bgdModel, fgdModel, 1, cv2.GC_INIT_WITH_MASK)
    
    mask = np.where((mask==2)|(mask==0),0,1).astype('uint8')

    img_result = color_image*mask[:,:,np.newaxis]

    if verbose == True:
        plt.imshow(mask)
        plt.show()

        plt.imshow(img_result, cmap=ListedColormap(colors))
        plt.axis('off')
        plt.show()
    
    return mask

def update_all_masks(expansion_result_mask, last_expansion_class,  initial_masks, updated_masks, verbose=False):
    '''
    loop through all the initial masks and revise the updated_masks with the most recent output
    of the grab and cut
    '''
    
    for key,init_mask in initial_masks.items():
        if key == last_expansion_class:

            updated_masks[key][initial_masks[key][:,:]==1.0] = 1 # definite foreground
            updated_masks[key][initial_masks[key][:,:]==3.0] = 3 # probable foreground

            updated_masks[key][(expansion_result_mask[:,:]==1.0) & (initial_masks[key][:,:]==3.0)] = 3 # probable foreground

            updated_masks[key][(expansion_result_mask[:,:]==0.0) & (initial_masks[key][:,:]==3.0)] = 2 # probable background

            if verbose == True:
                plt.imshow(initial_masks[key], cmap=ListedColormap(colors))
                plt.title(key)
                plt.tight_layout()
                plt.show()

                plt.imshow(updated_masks[key], cmap=ListedColormap(colors))
                plt.title(str(key) + " new mask")
                plt.colorbar()
                plt.tight_layout()
                plt.show()

        elif key != last_expansion_class:

            updated_masks[key][initial_masks[key][:,:]==1.0] = 1 # definite foreground
            updated_masks[key][initial_masks[key][:,:]==3.0] = 3 # probable foreground

            updated_masks[key][(expansion_result_mask[:,:]==0.0) & (initial_masks[key][:,:]==3.0)] = 3 # probable foreground

            updated_masks[key][(expansion_result_mask[:,:]==1.0) & (initial_masks[key][:,:]==3.0)] = 2 # probable background

            if verbose == True:
                plt.imshow(initial_masks[key], cmap=ListedColormap(colors))
                plt.title(key)
                plt.tight_layout()
                plt.show()

                plt.imshow(updated_masks[key], cmap=ListedColormap(colors))
                plt.title(str(key) + " new mask")
                plt.colorbar()
                plt.tight_layout()
                plt.show()

#             updated_masks[key] = updated_masks[key].astype('uint8')
            
        else:
            print("I should not be here")
            break
        
    return(updated_masks)
    
    
def output_mask(mask_dict, class_label_dict,verbose=False):
    # get mask dimensions
    first_label = [i for i in mask_dict.keys()][0]
    mask_dims = mask_dict[first_label].shape
    
    # get image dims
    H,W,C = mask_dims[0], mask_dims[1], 3

    # create empty mask
    final_mask = np.ones((H,W,C))  * 255
    
    # replace empty mask with areas of definite foreground from our mask merging procedure
    for k,v in mask_dict.items():

        final_mask[(v[:,:]==1) | (v[:,:]==3)] = class_label_dict[k]
  
    final_mask = final_mask.astype('uint8')
    
    if verbose==True:
        plt.imshow(final_mask)
        plt.title('Final Mask')
        plt.tight_layout()
        plt.show()  
    
    return final_mask

def create_ckeckpoint(updated_masks, verbose = False):
    
    checkpoint_masks = dict()
    
    for k,v in updated_masks.items():

        checkpoint_masks.update({k: np.zeros(v.shape).astype('uint8')})
    
    for k_1,v_1 in updated_masks.items():

        # if definitie or probable foreground, set to definite foreground
        checkpoint_masks[k_1][(v_1[:,:]==1) | (v_1[:,:]==3)] = 1
        
        # if probable background, set to probable foreground
        checkpoint_masks[k_1][(v_1[:,:]==2)] = 3

        for k_2,v_2 in updated_masks.items():
            
            if k_2 != k_1:
                
#                 # label cannot occupy the agreed upon label by two truthers
#                 checkpoint_masks[k_1][(v_2[:,:]==1)] = 0
                
                # if probable foreground in v_2, and probable background in v_1, set to probable background
                checkpoint_masks[k_1][(v_1[:,:]==2) & (v_2[:,:]==3)] = 2
                
            else:
                continue
        
    if verbose == True:
        for k,v in checkpoint_masks.items():
            plt.imshow(v, cmap=ListedColormap(colors))
            plt.title(k)
            plt.colorbar()
            plt.tight_layout()
            plt.show()
        
    return checkpoint_masks





def process_two_gt(pseudoColor,gt1,gt2,saveTo=None,threshold=0.5,num_epochs=10):
    """
    input should be one pseudoColor image, the corresponding two truthed images
    """
    # Load the iamge files
    img = cv2.imread(pseudoColor)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
#    img = img[30:, :-30] # H W
    
    mask_A = cv2.imread(gt1)
    mask_A = cv2.cvtColor(mask_A, cv2.COLOR_BGR2RGB)
#    mask_A = mask_A[30:, :-30] # H W
    
    mask_B = cv2.imread(gt2)
    mask_B = cv2.cvtColor(mask_B, cv2.COLOR_BGR2RGB)
#    mask_B = mask_B[30:, :-30] # H W
        
    # identify the classes in the GT iamges
    labels_in_images = []
    H,W = mask_B.shape[0], mask_B.shape[1]
    
    for label in rotation_order:
        in_image_A = np.sum(np.all(mask_A.reshape(-1, mask_A.shape[2])== label_dict[label], axis=1))
        in_image_B = np.sum(np.all(mask_B.reshape(-1, mask_A.shape[2])== label_dict[label], axis=1))
        
        
        mask_A_label = np.where(mask_A.reshape(-1, mask_A.shape[2])== label_dict[label],1,0)
        mask_B_label = np.where(mask_B.reshape(-1, mask_A.shape[2])== label_dict[label],1,0)
        
        new_mask_A = mask_A_label[:,0].reshape(H,W)
        new_mask_B = mask_B_label[:,0].reshape(H,W)
        
        sum_mask_A = np.sum(new_mask_A)
        sum_mask_B = np.sum(new_mask_B)
        
        
        if sum_mask_A > 0 or sum_mask_B > 0:
            iou = 1.0 * np.sum(new_mask_A * new_mask_B) / (sum_mask_A + sum_mask_B - np.sum(new_mask_A * new_mask_B))
            if iou < threshold:
                print('Error: The overlap area for category {} between the two GT images is too low'.format(label))
                return False
        
        
        if in_image_A>0 or in_image_B>0:
            labels_in_images.append(label)
        
        
        # rotation order is retained in the variable labels_in_images, so we can use it going forward
        
    print("labels found in GT images:")
    print(labels_in_images)
    
    # Initialize the grab and cut masks for each class label
    my_initial_masks = dict()
    
    for label in labels_in_images:
    
        init_mask = create_initial_masks(mask_A, mask_B, label)
    
        my_initial_masks.update({label:init_mask})
    
    
    # Initialize the update grab and cut masks for each class label
    my_updated_masks = my_initial_masks
    
    
    # perform Grab and Cut in order of rotation_order
    num_epochs = num_epochs
    iterations = list(range(len(labels_in_images)))*num_epochs
    for i in iterations:
        
        current_GC_mask = my_updated_masks[labels_in_images[i]]
        
        GC_mask = perform_grab_cut(img, current_GC_mask)
        
        my_updated_masks = update_all_masks(GC_mask, labels_in_images[i],  my_initial_masks, my_updated_masks, verbose = False)
        
    my_updated_masks = create_ckeckpoint(my_updated_masks, verbose = False)
    
    for i in iterations:
        
        current_GC_mask = my_updated_masks[labels_in_images[i]]
        
        GC_mask = perform_grab_cut(img, current_GC_mask)
        
        my_updated_masks = update_all_masks(GC_mask, labels_in_images[i],  my_initial_masks, my_updated_masks, verbose = False)
        
    my_updated_masks = create_ckeckpoint(my_updated_masks, verbose = False)
    
    for i in iterations:
        
        current_GC_mask = my_updated_masks[labels_in_images[i]]
        
        GC_mask = perform_grab_cut(img, current_GC_mask)
        
        my_updated_masks = update_all_masks(GC_mask, labels_in_images[i],  my_initial_masks, my_updated_masks, verbose = False)
        
    my_updated_masks = create_ckeckpoint(my_updated_masks, verbose = False)
    
    for i in iterations:
        
        current_GC_mask = my_updated_masks[labels_in_images[i]]
        
        GC_mask = perform_grab_cut(img, current_GC_mask)
        
        my_updated_masks = update_all_masks(GC_mask, labels_in_images[i],  my_initial_masks, my_updated_masks, verbose = False)
        
    
    out_mask = output_mask(my_updated_masks, label_dict)
    
    
    if saveTo:
        cv2.imwrite(saveTo, cv2.cvtColor(out_mask, cv2.COLOR_RGB2BGR))
    else:
        cv2.imwrite("merged_mask.png", cv2.cvtColor(out_mask, cv2.COLOR_RGB2BGR))
        
    print('Well Done')
    return True


data = {
    "PrimaryDoctor": "s3://wakeforest/sagetest/output/wakeforest-Subject02-1020Burn1ID418294/manifests/output/output.manifest",
    "SecondaryDoctor": "s3://wakeforest/sagetest/output/wakeforest-Subject02-1020Burn1ID259896/manifests/output/output.manifest"
}

# Resources and Clients
s3_client = s3 = boto3.client('s3')

def translate_s3_uri(uri):
    
    bucket = uri.split('/')[2]
    key = '/'.join(uri.split('/')[3::])

    return bucket, key

def image_from_s3(bucket, key):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    image = bucket.Object(key)
    img_data = image.get().get('Body').read()

    # file_name = key.split('/')[-1]
    # image_save = Image.open(BytesIO(img_data))

    # file_location = f'C:/tmp/{random.randint(0, 10000)}.png'
    # image_save.save(file_location)
    
    # return Image.open(file_location)

    return Image.open(BytesIO(img_data))
    
def lambda_handler(event,context):
    
    # Extract data from the job

    extracted_data = re.findall(r'([0-9]+)', event['PrimaryDoctor'])
    subject_id = '-'.join([extracted_data[1], extracted_data[2]])
    wound = extracted_data[3]
    subject_wound = '_'.join([subject_id, wound])

    primary_job_id = extracted_data[4]
    extracted_data = re.findall('([0-9]+)', event['SecondaryDoctor'])
    secondary_job_id = extracted_data[4]
    
    print(subject_wound)
    print(primary_job_id)
    print(secondary_job_id)

    # Set variables
    primary_raw_location= event['PrimaryDoctor']
    secondary_raw_location= event['SecondaryDoctor']

    primary_bucket, primary_key = translate_s3_uri(primary_raw_location)
    secondary_bucket, secondary_key = translate_s3_uri(secondary_raw_location)
    
    primary_manifest_object = s3_client.get_object(
        Bucket=primary_bucket, 
        Key=primary_key)

    secondary_manifest_object = s3_client.get_object(
        Bucket=secondary_bucket, 
        Key=secondary_key)


    primary_raw_manifest = primary_manifest_object['Body'].read().decode('utf-8')
    secondary_raw_manifest = secondary_manifest_object['Body'].read().decode('utf-8')

    # We will use the primary doctor's pseudo images as the input and generate a dictionary
    # This dictionary will contain the list of both overlays per pseudo color
    final_manifest = {}

    for manifest_line in primary_raw_manifest.splitlines():

        # Load json from manifest
        primary_manifest_json = json.loads(manifest_line)

        # Get the name of the SageMaker overlay
        for key in primary_manifest_json.keys():
            if 'SageMaker' in key and 'metadata' not in key:
                job_name = key

        if not primary_manifest_json['source-ref'] in final_manifest:
            final_manifest[primary_manifest_json['source-ref']] = [primary_manifest_json[job_name]]
        elif len(final_manifest[primary_manifest_json['source-ref']]) < 1:
            final_manifest[primary_manifest_json['source-ref']].append(primary_manifest_json[job_name])
    
    # Do the same for the seconary doctor
    for manifest_line in secondary_raw_manifest.splitlines():

        # Load json from manifest
        secondary_manifest_json = json.loads(manifest_line)

        # Get the name of the SageMaker overlay
        for key in secondary_manifest_json.keys():
            if 'SageMaker' in key and 'metadata' not in key:
                job_name = key

        if not secondary_manifest_json['source-ref'] in final_manifest:
            final_manifest[secondary_manifest_json['source-ref']] = [secondary_manifest_json[job_name]]
        elif len(final_manifest[secondary_manifest_json['source-ref']]) < 2:
            final_manifest[secondary_manifest_json['source-ref']].append(secondary_manifest_json[job_name])

        

    # pseudo_bucket, pseudo_key = translate_s3_uri(manifest_json['source-ref'])
    # mask_bucket, mask_key = translate_s3_uri(manifest_json[job_name]
    print(final_manifest)


if __name__ == "__main__":

    lambda_handler(data, None)
