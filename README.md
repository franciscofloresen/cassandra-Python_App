# cassandra-Python_App
A data base app for Cassandra in Python

Métodos Utilizados para el Desarrollo del Proyecto

Herramientas

● Apache Cassandra: Seleccionada por su modelo de datos flexible y su capacidad para escalar horizontalmente, facilitando el manejo de grandes cantidades de datos distribuidos.

● Docker: Utilizado para contenerizar el entorno de desarrollo y producción, asegurando la consistencia entre diferentes entornos y simplificando el proceso de despliegue.

● Python: Elegido por su sintaxis clara y su extenso ecosistema de librerías, incluyendo el soporte para interactuar con Cassandra a través del driver oficial de DataStax.


Diseño e Implementación

● Diseño de la Base de Datos: Se tomó el análisis del laboratorio 2 para definir las tablas y las relaciones, optimizando el esquema para las consultas más frecuentes y garantizando un acceso eficiente a los datos.

● Contenerización y Despliegue: Se creó un archivo Dockerfile para Cassandra y se utilizó Docker Compose para manejar la aplicación y la base de datos como servicios conectados.

● Desarrollo de Scripts en Python: Se implementaron scripts para la creación de tablas, inserción de datos de prueba y ejecución de consultas, practicando la interacción programática con la base de datos.


Automatización y Pruebas

● Se desarrollaron scripts para automatizar la inicialización de la base de datos, la inserción de datos y la ejecución de consultas, facilitando la repetición de pruebas y la demostración de funcionalidades.
