#!/bin/bash
cd /home/softiago/miniconda3/bin/
source activate base
python /var/www/unab_script/sgt_demonio_no_validate.py
conda deactivate
