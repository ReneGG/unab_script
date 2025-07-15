# Este repositorio se usará para generar los script para el procesamiento automatico de los servicios
# de las aplicaciones SGR y SGT.

# Los archivos sh no se deben editar, es recomendable agregarlos al gi ignore y genera un txt para usarlo

# Script activos en crontab
sgr_ejecuta_cambio_estado.sh 
    Este script llama al archivo demonio_change_state.py, el cual cambia los estados de las reservas de Aceptada a Iniciada y de Iniciada a Finalizada, este archivo no se encuentra en el gitignore, por lo que NO SE DEBE EDITAR

sgt_gps_perez_lillo.sh
    Este script llama al archivo sgt_demonio_gps_perez_lillo.py, el cual cambia registra el GPS de la empresa Perez Lillo