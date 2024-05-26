from sqlalchemy import create_engine, Column, Integer, String, Float, Date, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd

# SQLAlchemy CONFIGURATION

engine = create_engine('sqlite:///tienda.db')
Base = declarative_base()
metadata = MetaData()

#Define the models
class Usuario(Base):
    __tablename__ = 'usuarios'
    id = Column(Integer, primary_key=True)
    nombre = Column(String)
    correo = Column(String)
    fecha_registro = Column(Date)

class Transaccion(Base):
    __tablename__ = 'transacciones'
    id = Column(Integer, primary_key=True)
    usuario_id = Column(Integer)
    monto = Column(Float)
    fecha = Column(Date)

# Drop the exists tables and them create a news tables 
Base.metadata.drop_all(engine)  # Drop tables
Base.metadata.create_all(engine)  # Create new tables

# Creat a sesion 
Session = sessionmaker(bind=engine)
session = Session()

#QUERY AND ANALYZE DATA

# Load data from CSV and prepare dates with date_format
usuarios_df = pd.read_csv('usuarios.csv', parse_dates=['fecha_registro'], date_format='%m/%d/%Y')
transacciones_df = pd.read_csv('transacciones.csv', parse_dates=['fecha'], date_format='%m/%d/%Y')

# Change date to object datetime.date
usuarios_df['fecha_registro'] = usuarios_df['fecha_registro'].dt.date
transacciones_df['fecha'] = transacciones_df['fecha'].dt.date

# Save data on data bases 
usuarios_df.to_sql('usuarios', engine, if_exists='append', index=False)
transacciones_df.to_sql('transacciones', engine, if_exists='append', index=False)


# Query data using pandas y SQLAlchemy
usuarios = pd.read_sql('usuarios', engine)
transacciones = pd.read_sql('transacciones', engine)

print("Usuarios:")
print(usuarios)
print("\nTransacciones:")
print(transacciones)


# Group by user and sum transaction amounts
transacciones_por_usuario = transacciones.groupby('usuario_id')['monto'].sum().reset_index()

# Join DataFrame with users to get names
result = pd.merge(transacciones_por_usuario, usuarios, left_on='usuario_id', right_on='id')
result = result[['nombre', 'monto']]
print("\nTotal de transacciones por usuario:")
print(result)


#Find the user with the highest total transaction amount
usuario_top = result.loc[result['monto'].idxmax()]
print("\nUsuario con mayor monto total de transacciones:")
print(usuario_top)

import matplotlib.pyplot as plt

# Graph the total amount of transactions per user
plt.figure(figsize=(10,6))
plt.bar(result['nombre'], result['monto'], color='skyblue')
plt.title('Monto Total de Transacciones por Usuario')
plt.xlabel('Usuario')
plt.ylabel('Monto Total')
plt.show()


