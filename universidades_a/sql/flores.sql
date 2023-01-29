SELECT 
    universidad,
    carrera, 
    fecha_de_inscripcion, 
    name, 
    sexo, 
    fecha_nacimiento,
    codigo_postal,
    direccion,
    correo_electronico
FROM flores_comahue 
WHERE universidad = 'UNIVERSIDAD DE FLORES' AND fecha_de_inscripcion BETWEEN '20200901' AND '20210201' 