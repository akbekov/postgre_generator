/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datageneratorpostgre;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author takbekov
 */
public class Database {

    //  данные для подключения
    private final String url = "jdbc:postgresql://localhost:5432/db_test2";
    private final String username = "postgres";
    private final String password = "root";

    //  функция подключения к базе данных
    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            System.out.println("Database:connection error:" + e.getLocalizedMessage());
            return null;
        }
    }

    //  функция для добавления строк в таблицу
    //  имя таблицы, перечень полей таблицы и значения полей передаются как параметры
    public void insert(String table, StringBuilder fields, StringBuilder values, Statement state) {
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("INSERT INTO ").append(table).append(" (").append(fields).append(") ");
            sql.append("VALUES (").append(values).append(")");
            state.executeUpdate(String.valueOf(sql));
        } catch (Exception e) {
            System.out.println("Database:insert error:" + e.getLocalizedMessage());
            System.out.println("Database:insert error:table:" + table + ":fields:" + fields + ":values:" + values);
        }
    }

    //  функция для удаления строк из таблицы
    //  имя таблицы, условия удаления строк передаются как параметры
    public void delete(String table, String condition, Statement state) {
        try {
            StringBuilder sql = new StringBuilder();
            sql.append("DELETE FROM ").append(table);
            if (condition != null) {
                sql.append("WHERE ").append(condition);
            }
            state.executeUpdate(String.valueOf(sql));
            System.out.println("Deleted rows:table:" + table);
        } catch (Exception e) {
            System.out.println("Database:delete error:" + e.getLocalizedMessage());
            System.out.println("Database:delete error:table:" + table + ":condition:" + condition);
        }
    }

    public List<Integer> select(String table, String field, String condition, String orderBy, Statement state) {
        try {
            List<Integer> id = new ArrayList<>();
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ").append(field);
            sql.append(" FROM ").append(table);
            if (condition != null) {
                sql.append("WHERE ").append(condition);
            }
            if (orderBy != null) {
                sql.append(" ORDER BY ").append(orderBy);
            }
            ResultSet rs = state.executeQuery(String.valueOf(sql));
            while (rs.next()) {
                id.add(rs.getInt(field));
            }
            rs.close();
            return id;
        } catch (Exception e) {
            System.out.println("Database:select error:" + e.getLocalizedMessage());
            System.out.println("Database:select error:table:" + table + ":condition:" + condition);
            return null;
        }
    }

}
