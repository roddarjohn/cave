INSERT INTO public.departments (id, name) VALUES (1, 'Engineering');
INSERT INTO public.departments (id, name) VALUES (2, 'Marketing');
INSERT INTO public.employees_attributes
(id, name, salary, department_id)
VALUES (1, 'Alice', 95000, 1);
INSERT INTO public.employees_attributes
(id, name, salary, department_id)
VALUES (2, 'Bob', 72000, 2);
INSERT INTO public.employees_root
(id, employees_attributes_id)
VALUES (1, 1);
INSERT INTO public.employees_root
(id, employees_attributes_id)
VALUES (2, 2);
