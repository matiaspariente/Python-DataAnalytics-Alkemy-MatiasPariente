SELECT 
	universidad,
    carrera, 
	fecha_de_inscripcion,
    nombre, 
    sexo, 
    fecha_nacimiento,
    localidad,
    direccion,
    email
FROM salvador_villa_maria 
WHERE universidad = 'UNIVERSIDAD_NACIONAL_DE_VILLA_MARÍA' AND fecha_de_inscripcion :: DATE BETWEEN '20200901' AND '20210201' 