import zipfile, os, fnmatch

root_path = input('Enter a directory to unzip: ')
dest_path = input('Enter a destination directory: ')
pattern = '*.zip'

for root, dirs, files in os.walk(root_path):
    for filename in fnmatch.filter(files, pattern):
        print(os.path.join(root, filename))
        zipfile.ZipFile(os.path.join(root, filename)).extractall(os.path.join(dest_path, os.path.splitext(filename)[0]))
