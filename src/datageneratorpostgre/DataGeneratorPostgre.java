/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package datageneratorpostgre;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

/**
 *
 * @author takbekov
 */
public class DataGeneratorPostgre {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        new DataGeneratorPostgre().run();
    }

    private final Database database = new Database();
    private final Random rand = new Random();

    private void run() {
        Properties props = loadProperties();
        if (props != null) {
            generate(props, database.getConnection());
        }
    }

    private Properties loadProperties() {
        try {
            Properties props = new Properties();
            InputStream input = new FileInputStream("config.properties");
            props.load(input);
            input.close();
            return props;
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:loadProperties:" + e.getLocalizedMessage());
            return null;
        }
    }

    private void generate(Properties props, Connection connection) {
        deleteData(connection);
        System.out.println("\nAll data deleted!\n");

        System.out.println("Generation start!\n");
        System.out.println("generateAirport:start:" + new Date());
        generateAirport(Integer.parseInt(props.getProperty("airport")), connection);
        System.out.println("generateAirport:finish:" + new Date() + "\n");

        System.out.println("generateTariff:start:" + new Date());
        generateTariff(Integer.parseInt(props.getProperty("tariff")), connection);
        System.out.println("generateTariff:finish:" + new Date() + "\n");

        System.out.println("generateAirline:start:" + new Date());
        generateAirline(Integer.parseInt(props.getProperty("airline")), connection);
        System.out.println("generateAirline:finish:" + new Date() + "\n");

        System.out.println("generatePassenger:start:" + new Date());
        generatePassenger(Integer.parseInt(props.getProperty("passenger")), connection);
        System.out.println("generatePassenger:finish:" + new Date() + "\n");

        System.out.println("generatePlane:start:" + new Date());
        generatePlane(Integer.parseInt(props.getProperty("plane")), connection);
        System.out.println("generatePlane:finish:" + new Date() + "\n");

        System.out.println("generateFlight:start:" + new Date());
        generateFlight(Integer.parseInt(props.getProperty("flight")), connection);
        System.out.println("generateFlight:finish:" + new Date() + "\n");

        System.out.println("generatePlaneHistory:start:" + new Date());
        generatePlaneHistory(Integer.parseInt(props.getProperty("plane_history")), connection);
        System.out.println("generatePlaneHistory:finish:" + new Date() + "\n");

        System.out.println("generateTicket:start:" + new Date());
        generateTicket(Integer.parseInt(props.getProperty("ticket")), connection);
        System.out.println("generateTicket:finish:" + new Date() + "\n");

        System.out.println("generatePlaneTariffs:start:" + new Date());
        generatePlaneTariffs(Integer.parseInt(props.getProperty("plane_tariffs")), connection);
        System.out.println("generatePlaneTariffs:finish:" + new Date() + "\n");

        System.out.println("generatePassengerHistory:start:" + new Date());
        generatePassengerHistory(Integer.parseInt(props.getProperty("passenger_history")), connection);
        System.out.println("generatePassengerHistory:finish:" + new Date() + "\n");

        System.out.println("Generation finish!");
        connection = null;
    }

    private void deleteData(Connection connection) {
        try {
            Statement state = connection.createStatement();
            database.delete("airport", null, state);
            database.delete("tariff", null, state);
            database.delete("airline", null, state);
            database.delete("passenger", null, state);
            database.delete("plane", null, state);
            database.delete("flight", null, state);
            database.delete("plane_history", null, state);
            database.delete("ticket", null, state);
            database.delete("plane_tariffs", null, state);
            database.delete("passenger_history", null, state);
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:deleteData:" + e.getLocalizedMessage());
        }
    }

    private String generateByString(String alphabet, int len) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < len; i++) {
            result.append(alphabet.charAt(rand.nextInt(alphabet.length())));
        }
        return result.toString();
    }

    private void generateAirport(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            //  считывание из файла стран и их столиц
            List<String> country = new ArrayList<>();
            List<String> city = new ArrayList<>();
            Scanner scan = new Scanner(new File("country and city.txt"));
            while (scan.hasNext()) {
                String line = scan.nextLine();
                country.add(line.substring(0, line.indexOf(":")).replaceAll("'", ""));
                city.add(line.substring(line.lastIndexOf(":") + 1).replaceAll("'", ""));
            }
            StringBuilder fields = new StringBuilder();
            fields.append("id_airport,contry,city,airport_name,timezone");
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                //  генерация строки из 3 символов. Возможные символы переданы как параметр
                String airportName = generateByString("QWERTYUIOPASDFGHJKLZXCVBNM", 3);
                int index = rand.nextInt(country.size());
                //  генерация часового пояса
                String zone = rand.nextInt(2) == 1 ? "+" : "-";
                String min = rand.nextInt(2) == 1 ? "30" : "00";
                int tz = rand.nextInt(12);
                String timezone;
                if (String.valueOf(tz).length() == 1) {
                    timezone = zone + "0" + tz + ":" + min;
                } else {
                    timezone = zone + tz + ":" + min;
                }
                //  передача сгенерированных данных в StringBuilder для добавления в таблицу
                values.append(i + 1).append(",'").append(country.get(index)).append("','")
                        .append(city.get(index)).append("','").append(airportName)
                        .append("','").append(timezone).append("'");
                database.insert("airport", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generateAirport:" + e.getLocalizedMessage());
        }
    }

    private void generateTariff(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_tariff,tar_name,tar_descrip,price");
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                values.append(i + 1).append(",'Tariff ").append(i + 1)
                        .append("','Description for Tariff ").append(i + 1).append("',")
                        .append(rand.nextInt(10000) / 100.0);
                database.insert("tariff", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generateTariff:" + e.getLocalizedMessage());
        }
    }

    private void generateAirline(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_airline,name,site,phone_no");
            List<String> airline = new ArrayList<>();
            Scanner scan = new Scanner(new File("airline.txt"));
            while (scan.hasNext()) {
                airline.add(scan.nextLine());
            }
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int index = rand.nextInt(airline.size());
                values.append(i + 1).append(",'").append(airline.get(index))
                        .append("','http://").append(airline.get(index).replaceAll(" ", "").toLowerCase()).append(".com")
                        .append("',").append(generateByString("0123456789", 14));
                database.insert("airline", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generateAirline:" + e.getLocalizedMessage());
        }
    }

    private void generatePassenger(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_passenger, name, surname, second_name, pasport_no, telephone_no, credcard_no");
            List<String> names = new ArrayList<>();
            List<String> surnames = new ArrayList<>();
            List<String> secondNames = new ArrayList<>();
            Scanner scanNames = new Scanner(new File("names.txt"));
            while (scanNames.hasNext()) {
                String line = scanNames.nextLine();
                names.add(line);
                secondNames.add(line);
            }
            Scanner scanSurnames = new Scanner(new File("surnames.txt"));
            while (scanSurnames.hasNext()) {
                surnames.add(scanSurnames.nextLine());
            }
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                values.append(i + 1).append(",'").append(names.get(rand.nextInt(names.size())))
                        .append("','").append(surnames.get(rand.nextInt(surnames.size())))
                        .append("','").append(secondNames.get(rand.nextInt(secondNames.size())))
                        .append("','").append("ID-").append(generateByString("0123456789", 6)) //  генерация строки из 6 символов, состоящей из символов 0123456789. Символы берутся рандомно
                        .append("',").append(generateByString("0123456789", 14))
                        .append(",").append(generateByString("0123456789", 16));
                database.insert("passenger", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generatePassenger:" + e.getLocalizedMessage());
        }
    }

    private void generatePlane(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_plane, plane_no, model, year, id_airline");
            List<String> plane = new ArrayList<>();
            List<Integer> year = new ArrayList<>();
            List<Integer> idAirline = database.select("airline", "id_airline", null, null, state);
            Scanner scan = new Scanner(new File("planes.txt"));
            while (scan.hasNext()) {
                plane.add(scan.nextLine());
            }
            for (int i = 1960; i < 2019; i++) {
                year.add(i);
            }
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int index = rand.nextInt(idAirline.size());
                int airline = idAirline.get(index);
                values.append(i + 1).append(",").append(rand.nextInt(5000))
                        .append(",'").append(plane.get(rand.nextInt(plane.size())))
                        .append("',").append(year.get(rand.nextInt(year.size())))
                        .append(",").append(airline);
                database.insert("plane", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generatePlane:" + e.getLocalizedMessage());
        }
    }

    private void generateFlight(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_flight, status, departure, arrival, dep_airport, arr_airport, id_airline");
            List<String> status = new ArrayList<>();
            List<Integer> idAirport = database.select("airport", "id_airport", null, null, state);
            List<Integer> idAirline = database.select("airline", "id_airline", null, null, state);
            Scanner scan = new Scanner(new File("status.txt"));
            while (scan.hasNext()) {
                status.add(scan.nextLine());
            }
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int indexAirline = rand.nextInt(idAirline.size());
                int airline = idAirline.get(indexAirline);

                int depAirport = idAirport.get(rand.nextInt(idAirport.size()));
                int arrAirport = idAirport.get(rand.nextInt(idAirport.size()));
                //  проверка: аэропорт вылета не может являться аэропортом прилета. В таком случае, генерируем новый id аэропорта
                while (depAirport == arrAirport) {
                    arrAirport = idAirport.get(rand.nextInt(idAirport.size()));
                }
                //  генерация даты с 1960 по 2017 годы. 631130400000 - 01.01.1960 в миллисекундах
                long ms = 631130400000L + (Math.abs(rand.nextLong()) % (27L * 365 * 24 * 60 * 60 * 1000));
                Date depart = new Date(ms);
                Calendar cal = Calendar.getInstance();
                cal.setTime(depart);
                //  добавление к времени прилета рандомного количества часов. Чтобы не получилось так, что самолет вылетел позже своего прилета
                cal.add(Calendar.HOUR_OF_DAY, rand.nextInt(24));
                Date arrival = cal.getTime();
                values.append(i + 1).append(",'").append(status.get(rand.nextInt(status.size())))
                        .append("','").append(depart)
                        .append("','").append(arrival)
                        .append("',").append(depAirport)
                        .append(",").append(arrAirport)
                        .append(",").append(airline);
                database.insert("flight", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generateFlight:" + e.getLocalizedMessage());
        }
    }

    private void generatePlaneHistory(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_plane_history, id_plane, id_flight");
            List<Integer> idPlane = database.select("plane", "id_plane", null, null, state);
            List<Integer> idFlight = database.select("flight", "id_flight", null, null, state);
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int index = rand.nextInt(idPlane.size());
                int plane = idPlane.get(index);
                idPlane.remove(index);  //  закомментировать чтобы разрешить повторение идентификаторов
                int index2 = rand.nextInt(idFlight.size());
                int flight = idFlight.get(index2);
                idFlight.remove(index2);  //  закомментировать чтобы разрешить повторение идентификаторов
                values.append(i + 1)
                        .append(",").append(plane)
                        .append(",").append(flight);
                database.insert("plane_history", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generatePlaneHistory:" + e.getLocalizedMessage());
        }
    }

    private void generateTicket(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_ticket, seat_no, flight_id");
            List<Integer> idFlight = database.select("plane", "id_plane", null, null, state);
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int seat = rand.nextInt(30) + 1;
                int index = rand.nextInt(idFlight.size());
                int flight = idFlight.get(index);
                values.append(i + 1)
                        .append(",").append(seat)
                        .append(",").append(flight);
                database.insert("ticket", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generateTicket:" + e.getLocalizedMessage());
        }
    }

    private void generatePlaneTariffs(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_plane_tariffs, id_plane, id_tariff, seats_num");
            List<Integer> idTariff = database.select("tariff", "id_tariff", null, null, state);
            List<Integer> idPlane = database.select("plane", "id_plane", null, null, state);
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int seat = rand.nextInt(30) + 1;
                int index = rand.nextInt(idPlane.size());
                int plane = idPlane.get(index);
                int index2 = rand.nextInt(idTariff.size());
                int tariff = idTariff.get(index2);
                idTariff.remove(index2);    //  закомментировать чтобы разрешить повторение идентификаторов
                values.append(i + 1)
                        .append(",").append(plane)
                        .append(",").append(tariff)
                        .append(",").append(seat);
                database.insert("plane_tariffs", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generatePlaneTariffs:" + e.getLocalizedMessage());
        }
    }

    private void generatePassengerHistory(int generate, Connection connection) {
        try {
            Statement state = connection.createStatement();
            StringBuilder fields = new StringBuilder();
            fields.append("id_pass_history, id_passenger, id_ticket");
            List<Integer> idTicket = database.select("ticket", "id_ticket", null, null, state);
            List<Integer> idPassenger = database.select("ticket", "id_ticket", null, null, state);
            for (int i = 0; i < generate; i++) {
                StringBuilder values = new StringBuilder();
                int index = rand.nextInt(idPassenger.size());
                int passenger = idPassenger.get(index);
                int index2 = rand.nextInt(idTicket.size());
                int ticket = idTicket.get(index2);
                idTicket.remove(index2);    //  закомментировать чтобы разрешить повторение идентификаторов
                values.append(i + 1)
                        .append(",").append(passenger)
                        .append(",").append(ticket);
                database.insert("passenger_history", fields, values, state);
            }
            state.close();
        } catch (Exception e) {
            System.out.println("DataGeneratorPostgre:generatePassengerHistory:" + e.getLocalizedMessage());
        }
    }

}
