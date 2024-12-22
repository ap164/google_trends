from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
import re  # Dodajemy moduł do pracy z wyrażeniami regularnymi
from datetime import datetime


app = Flask(__name__)

# Konfiguracja połączenia z PostgreSQL
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Definicja modelu Klienci
class Klienci(db.Model):
    __tablename__ = 'klienci'
    id_klienta = db.Column(db.Integer, primary_key=True, autoincrement=True)
    id_kampanii = db.Column(db.Integer)
    nazwa_klienta = db.Column(db.String(100))
    typ_klienta = db.Column(db.String(10))
    nip_klienta = db.Column(db.Integer)
    osoba_kontaktowa = db.Column(db.String(100))
    numer_kontaktowy = db.Column(db.String(20))
    mail_kontaktowy = db.Column(db.String(100))
    data_dodania_klienta = db.Column(db.DateTime(timezone=True), server_default=func.now(), nullable=False)

    kampanie = db.relationship('Products', backref='klient', lazy=True)

# Definicja modelu DagRuns
class DagRuns(db.Model):
    __tablename__ = 'cele_kampanii'
    id_celu = db.Column(db.Integer, primary_key=True)
    nazwa_celu = db.Column(db.String)

# Definicja modelu Products
class Products(db.Model):
    __tablename__ = 'kampanie'
    id_kampanii = db.Column(db.Integer, primary_key=True, autoincrement=True)
    nazwa_kampanii = db.Column(db.String(100), nullable=False)
    data_utworzenia_wpisu = db.Column(db.DateTime(timezone=True), server_default=func.now(), nullable=False)
    data_startu_kampanii = db.Column(db.Date, nullable=True)
    data_konca_kampanii = db.Column(db.Date, nullable=True)
    cel_kampanii = db.Column(db.Integer, db.ForeignKey('cele_kampanii.id_celu'), nullable=True)
    id_klienta = db.Column(db.Integer, db.ForeignKey('klienci.id_klienta'), nullable=True)  # Klucz obcy
    budzet_kampanii = db.Column(db.Float, nullable=True)
    waluta_rozliczenia = db.Column(db.String(15), nullable=False)
    id_opiekuna = db.Column(db.Integer, nullable=True)
    status_kampanii = db.Column(db.String(30), nullable=False)

    dag_run = db.relationship('DagRuns', backref='kampanie')


# Funkcja walidująca numer telefonu
def is_valid_phone_number(numer_kontaktowy):
    phone_regex = r'^\+?[0-9]{9,15}$'  # Numer telefonu musi mieć od 9 do 15 cyfr
    return re.match(phone_regex, numer_kontaktowy)


@app.route('/', methods=['GET', 'POST'])
def form():
    client_found = None
    client_not_found = False
    current_client = None
    dag_runs = DagRuns.query.all()  # Pobieranie dag_runs z bazy danych

    zasada_dat_koniec_start = False
    zly_numer = False  # Dodajemy zmienną do walidacji numeru po stronie backendu
    product_added_successfully = None
    if request.method == 'POST':
        # Krok 1: Sprawdzenie, czy klient istnieje
        if 'client_exists' in request.form:
            client_exists = request.form['client_exists']

            if client_exists == 'yes':
                client_found = True  # Przejdź do formularza wyszukiwania
            else:
                client_found = False  # Przejdź do formularza dodawania klienta

            return render_template('form.html', client_found=client_found, client_not_found=client_not_found, dag_runs=dag_runs)

        # Logika wyszukiwania klienta
        if 'search_client' in request.form:
            search_option = request.form['search_option']
            search_value = request.form['search_value']

            # Wyszukiwanie klienta po ID lub nazwie
            if search_option == 'id_klienta':
                client = db.session.query(Klienci).filter_by(id_klienta=search_value).first()
            elif search_option == 'nazwa_klienta':
                client = db.session.query(Klienci).filter_by(nazwa_klienta=search_value).first()

            if client:
                current_client = client
            else:
                client_not_found = True  # Wyświetl komunikat "nie znaleziono"
                client_found = True  # Kontynuuj wyświetlanie formularza wyszukiwania

        # Logika dodawania nowego klienta
        if 'add_client' in request.form:
            nowy_nazwa_klienta = request.form['nowy_nazwa_klienta']
            nowy_typ_klienta = request.form['nowy_typ_klienta']
            nowy_nip_klienta = request.form['nowy_nip_klienta']
            nowy_osoba_kontaktowa = request.form['nowy_osoba_kontaktowa']
            nowy_numer_kontaktowy = request.form['nowy_numer_kontaktowy']
            nowy_mail_kontaktowy = request.form['nowy_mail_kontaktowy']

            # Sprawdzenie poprawności numeru telefonu
            if not is_valid_phone_number(nowy_numer_kontaktowy):
                zly_numer = True
                # Ponownie renderujemy formularz z komunikatem o błędzie
                return render_template('form.html', client_found=client_found, client_not_found=client_not_found, dag_runs=dag_runs, zly_numer=zly_numer)

            # Jeśli numer jest poprawny, dodajemy klienta do bazy danych
            new_client = Klienci(
                nazwa_klienta = nowy_nazwa_klienta,
                typ_klienta = nowy_typ_klienta,
                nip_klienta = nowy_nip_klienta,
                osoba_kontaktowa = nowy_osoba_kontaktowa,
                numer_kontaktowy = nowy_numer_kontaktowy,
                mail_kontaktowy = nowy_mail_kontaktowy,
            )

            db.session.add(new_client)
            db.session.commit()
            current_client = new_client

        # Logika dodawania produktu dla klienta
        if 'add_product' in request.form:
            # Pobierz client_id z ukrytego pola formularza
            client_id = request.form.get('client_id')

            if client_id:
                current_client = db.session.get(Klienci, client_id)
            else:
                current_client = None

            if current_client:
                nowa_nazwa_kampanii = request.form['nowa_nazwa_kampanii']
                nowa_data_startu_kampanii = request.form['nowa_Data_startu_kampanii']
                nowa_data_konca_kampanii = request.form['nowa_Data_konca_kampanii']
                nowy_cel_kampanii = request.form['nowy_Cel_kampanii']
                nowy_budzet_kampanii = request.form['nowy_Budzet_kampanii']
                nowa_waluta_rozliczenia = request.form['nowa_Waluta_rozliczenia']
                nowy_status_kampanii = 'Zarejestrowano'

                # Konwertuj daty na obiekty datetime
                start_date = datetime.strptime(nowa_data_startu_kampanii, '%Y-%m-%d')
                end_date = datetime.strptime(nowa_data_konca_kampanii, '%Y-%m-%d')

                # Sprawdzenie poprawności dat
                if start_date >= end_date:
                    zasada_dat_koniec_start = True  # Ustaw flaga błędu, daty są nieprawidłowe
                    return render_template('form.html', 
                                            current_client=current_client, 
                                            zasada_dat_koniec_start=zasada_dat_koniec_start)

                # Jeśli walidacja przejdzie, dodajemy produkt do bazy danych
                new_product = Products(
                    nazwa_kampanii=nowa_nazwa_kampanii,
                    data_startu_kampanii=nowa_data_startu_kampanii,
                    data_konca_kampanii=nowa_data_konca_kampanii,
                    cel_kampanii=nowy_cel_kampanii,
                    id_klienta=current_client.id_klienta,
                    budzet_kampanii=nowy_budzet_kampanii,
                    waluta_rozliczenia=nowa_waluta_rozliczenia,
                    status_kampanii=nowy_status_kampanii
                )

                db.session.add(new_product)
                db.session.commit()
                product_added_successfully = True
    return render_template('form.html', client_found=client_found, 
                           client_not_found=client_not_found, 
                           current_client=current_client, 
                           dag_runs=dag_runs, 
                           zasada_dat_koniec_start=zasada_dat_koniec_start,
                           zly_numer=zly_numer,
                           product_added_successfully=product_added_successfully)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
