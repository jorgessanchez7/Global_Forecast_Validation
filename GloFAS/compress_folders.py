import os
import shutil

# Directorio que quieres comprimir
source_dir = r'E:\GloFAS_Forecast\20250731'

# Nombre del archivo de salida (sin la extensión)
output_filename = r'E:\GloFAS_Forecast\20250731'

# Crea el archivo comprimido en formato .tar.gz
shutil.make_archive(output_filename, 'gztar', source_dir)

print(f"Carpeta '{source_dir}' comprimida en '{output_filename}.tar.gz'")