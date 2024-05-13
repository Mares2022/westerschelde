import os
import imageio.v2 as imageio

def create_animation(folder_path, images_end,animation_name):
    files_in_folder = os.listdir(folder_path)
    filtered_files = [file for file in files_in_folder if file.endswith(images_end)]

    filtered_files.sort()

    path_list = []
    for file in filtered_files:
        path = os.path.join(folder_path , file)
        path_list.append(path)
        print(path)

    #output_filename = folder_path + '/ch_animation.mp4'
    output_filename = '/home/eodc/PLOTS/' + animation_name

    writer = imageio.get_writer(output_filename, duration=300) 
    #writer = imageio.get_writer(output_filename)

    # Loop through the image paths and add frames to the animation
    for image_path in path_list:
            img = imageio.imread(image_path)
            writer.append_data(img)

    # Close the writer to save the animation
    writer.close()

    print(f'Animation saved as {output_filename}')

folder_path  = '/eodc/private/deltares/OUTPUT_10m_2015_2024'  

images_end_l2w  = 'L2W_chl_re_gons.png'
images_end_l1c  = 'L1R_rgb_rhot.png'
images_end_l2a  = 'L2R_rgb_rhos.png'

animation_name_l2w = '/ch_animation.gif'
animation_name_l1c = '/l1c_animation.gif'
animation_name_l2a = '/l2a_animation.gif'

create_animation(folder_path, images_end_l2w, animation_name_l2w)
create_animation(folder_path,images_end_l1c, animation_name_l1c)
create_animation(folder_path,images_end_l2a, animation_name_l2a)