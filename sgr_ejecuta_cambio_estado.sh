#!/bin/bash
cd /home/softiago/miniconda3/bin/
source activate base
python /var/www/unab_script/demonio_change_state.py
conda deactivate
