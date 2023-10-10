#!/bin/bash
cd /home/softiago/miniconda3/bin/
source activate base
python /var/www/unab_script/demonio_student.py
conda deactivate
