# DistribuidosI-TP1
Trabajo practico 1 de 75.74 Sistemas Distribuidos I - FIUBA

Para poder ejecutar el sistema completo se provee un archivo de Makefile y un archivo de docker.compose. Para levantar el sistema se debe ejecutar "make docker-compose-up" y luego "make  docker-compose-logs" para poder ir viendo los logs del sistema. Cada entidad en ejecución loggea cierta data que permite comprender el estado del sistema al momento de loggeear. La entidad FileReader, al obtener los resultados, los loggeara por pantalla. Para poder apagar el sistema se debe ejecutar "make docker-compose-down".

Las entidades replicables son los filtros, los brokers y los EJTsolvers. El resto de las entidades no son replicables y no deben replicarse para evitar un funcionamiento incorrecto del sistema. Las entidades replicables son aquellas que consumiran y procesaran la data. En un entorno productivo podrian replicarse correctamente para escalar el sistema.

De replicar las entidades mencionadas, se debe informar en las variables de entorno de otras entidades esta replicación, como se detalla a continuación:

- De replicar algunos de los brokers: se debe informar la cant de brokers de weather en la entidad entry_point y EofListener, en la env WBRKCANT. Se debe informar la cant de brokers de stations en la entidad Ej2Solver, entry_point y EofListener en la env SBRKCANT. Se debe informar la cant de brokers de trips en la entidad Ej2Solver, EofTListener y EofListener en la env TBRKCANT.

- De replicar algunos de los filters, se debe informar en las variables de entorno de ciertas entidades de la siguiente manera: de replicar el filtro de weathers para el ej1, "we1", informar en la entidad Ej1Solver y EofListener en la env WE1FCANT. De replicar el filtro de estaciones para ej3, "se3", informar en la entidad Ej3Solver y EofListener, en la env SE3FCANT. De replicar filtro de trips para ej2, "te2", informar en la entidad EofTListener y EofListener, en la env TE2FCANT. De replicar filtro de trips para ej3, "te3", informar en la entidad EofTListener y EofListener, en la env TE3FCANT.

- De replicar algunos de los EJTsolver, se debe informar de la siguiente manera: De replicar EJ1Tsolver, se debe informar en Ej1Solver y en EofTListener en la env EJ1TCANT. De replicar EJ2Tsolver, se debe informar en Ej2Solver y en EofTListener en la env EJ2TCANT. De replicar EJ3Tsolver, se debe informar en Ej3Solver y en EofTListener en la env EJ3TCANT. 


Ademas, se aclara que si se quiere correr el sistema se deben modificar dos rows del archivo de stations de toronto. La 691 y la 822 tienen caracteres invalidos que no pueden procesarse correctamente. La 691 debe modificarse el caracter invalido por un "–" y la 822 por un "’".
