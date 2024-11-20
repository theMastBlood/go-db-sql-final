package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	query := "INSERT INTO parcel (client, status, address, created_at) VALUES (?, ?, ?, ?)"
	res, err := s.db.Exec(query, p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		fmt.Println(err)
		return 0, nil
	}

	// верните идентификатор последней добавленной записи
	id, err := res.LastInsertId()
	if err != nil {
		fmt.Println(err)
		return 0, nil
	}
	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	query := "SELECT client, status, address, created_at FROM parcel WHERE number = ?"
	row := s.db.QueryRow(query, number)

	var (
		client    int
		status    string
		address   string
		createdAt string
	)

	err := row.Scan(&client, &status, &address, &createdAt)
	if err != nil {
		return Parcel{}, err
	}

	// заполните объект Parcel данными из таблицы
	p := Parcel{
		Number:    number,
		Client:    client,
		Status:    status,
		Address:   address,
		CreatedAt: createdAt,
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	query := "SELECT number, client, status, address, created_at FROM parcel WHERE client = ?"
	rows, err := s.db.Query(query, client)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer rows.Close()
	// заполните срез Parcel данными из таблицы
	var res []Parcel

	for rows.Next() {
		parcel := Parcel{}

		err := rows.Scan(&parcel.Number, &parcel.Client, &parcel.Status, &parcel.Address, &parcel.CreatedAt)
		if err != nil {
			return nil, err
		}

		res = append(res, parcel)
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	query := "UPDATE parcel SET status = ? WHERE number = ?"
	_, err := s.db.Exec(query, status, number)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	var status string
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number",
		sql.Named("number", number))
	err := row.Scan(&status)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if status == ParcelStatusRegistered {
		_, err := s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number",
			sql.Named("address", address),
			sql.Named("number", number))
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	var status string
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number",
		sql.Named("number", number))
	err := row.Scan(&status)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if status == ParcelStatusRegistered {
		_, err := s.db.Exec("DELETE FROM parcel WHERE number = :number",
			sql.Named("number", number))
		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}
