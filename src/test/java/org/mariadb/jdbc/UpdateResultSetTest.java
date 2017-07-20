/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc;

import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class UpdateResultSetTest extends BaseTest {

    /**
     * Test error message when no primary key.
     *
     * @throws Exception not expected
     */
    @Test
    public void testNoPrimaryKey() throws Exception {
        createTable("testnoprimarykey", "`id` INT NOT NULL,"
                + "`t1` VARCHAR(50) NOT NULL");
        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO testNoPrimaryKey VALUES (1, 't1'), (2, 't2')");

        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement(
                "SELECT * FROM testnoprimarykey", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery("SELECT * FROM testnoprimarykey");
            rs.next();
            try {
                rs.updateString(1, "1");
                fail();
            } catch (SQLException sqle) {
                assertEquals("ResultSet cannot be updated/deleted. Table "
                        + "`" + sharedConnection.getCatalog() + "`.`testnoprimarykey` has no primary key", sqle.getMessage());
            }
        }
    }

    @Test
    public void testNoDatabase() throws Exception {
        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement(
                "SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            try {
                rs.updateString(1, "1");
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("ResultSet cannot be updated/deleted. Unknown database"));
            }

        }
    }


    @Test
    public void testUpdateWithoutPrimary() throws Exception {
        createTable("UpdateWithoutPrimary", "`id` INT NOT NULL AUTO_INCREMENT,"
                + "`t1` VARCHAR(50) NOT NULL,"
                + "`t2` VARCHAR(50) NULL default 'default-value',"
                + "PRIMARY KEY (`id`)");

        Statement stmt = sharedConnection.createStatement();
        stmt.executeQuery("INSERT INTO UpdateWithoutPrimary(t1,t2) values ('1-1','1-2')");

        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT t1, t2 FROM UpdateWithoutPrimary",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            try {
                rs.updateString(1, "1-1-bis");
                rs.updateRow();
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("ResultSet cannot be updated/deleted. Primary key "
                        + "field `id` is not in result-set"));
            }
            try {
                rs.deleteRow();
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("ResultSet cannot be updated/deleted. Primary key "
                        + "field `id` is not in result-set"));
            }
        }

        ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM UpdateWithoutPrimary");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("1-1", rs.getString(2));
        assertEquals("1-2", rs.getString(3));

        assertFalse(rs.next());
    }

    @Test
    public void testUpdateWithPrimary() throws Exception {
        createTable("testUpdateWithPrimary", "`id` INT NOT NULL AUTO_INCREMENT,"
                + "`t1` VARCHAR(50) NOT NULL,"
                + "`t2` VARCHAR(50) NULL default 'default-value',"
                + "PRIMARY KEY (`id`)", "DEFAULT CHARSET=utf8");

        Statement stmt = sharedConnection.createStatement();
        stmt.executeQuery("INSERT INTO testUpdateWithPrimary(t1,t2) values ('1-1','1-2'),('2-1','2-2')");

        String utf8escapeQuote = "你好 '\' \" \\";

        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT id, t1, t2 FROM testUpdateWithPrimary",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();

            rs.moveToInsertRow();
            rs.updateInt(1, -1);
            rs.updateString(2, "0-1");
            rs.updateString(3, "0-2");
            rs.insertRow();

            rs.next();
            rs.next();
            rs.updateString(2, utf8escapeQuote);
            rs.updateRow();
        }

        ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM testUpdateWithPrimary");
        assertTrue(rs.next());
        assertEquals(-1, rs.getInt(1));
        assertEquals("0-1", rs.getString(2));
        assertEquals("0-2", rs.getString(3));

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("1-1", rs.getString(2));
        assertEquals("1-2", rs.getString(3));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(utf8escapeQuote, rs.getString(2));
        assertEquals("2-2", rs.getString(3));

        assertFalse(rs.next());
    }

    @Test
    public void testPrimaryGenerated() throws Exception {
        createTable("PrimaryGenerated", "`id` INT NOT NULL AUTO_INCREMENT,"
                + "`t1` VARCHAR(50) NOT NULL,"
                + "`t2` VARCHAR(50) NULL default 'default-value',"
                + "PRIMARY KEY (`id`)");

        Statement stmt = sharedConnection.createStatement();

        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT t1, t2 FROM PrimaryGenerated",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();

            rs.moveToInsertRow();
            rs.updateString(1, "1-1");
            rs.updateString(2, "1-2");
            rs.insertRow();

            rs.moveToInsertRow();
            rs.updateString(1, "2-1");
            rs.insertRow();

            rs.moveToInsertRow();
            rs.updateString(2, "3-2");
            try {
                rs.insertRow();
                fail("must not occur since t1 cannot be null");
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("Field 't1' doesn't have a default value"));
            }
        }

        ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM PrimaryGenerated");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("1-1", rs.getString(2));
        assertEquals("1-2", rs.getString(3));

        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals("2-1", rs.getString(2));
        assertEquals("default-value", rs.getString(3));

        assertFalse(rs.next());
    }


    @Test
    public void testPrimaryGeneratedDefault() throws Exception {
        createTable("testPrimaryGeneratedDefault", "`id` INT NOT NULL AUTO_INCREMENT,"
                + "`t1` VARCHAR(50) NOT NULL default 'default-value1',"
                + "`t2` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                + "PRIMARY KEY (`id`)");

        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT t1, t2 FROM testPrimaryGeneratedDefault",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();
            rs.next();
            rs.moveToInsertRow();
            rs.insertRow();
        }

        Statement stmt = sharedConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id, t1, t2 FROM testPrimaryGeneratedDefault");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals("default-value1", rs.getString(2));
        assertNotNull(rs.getDate(3));

        assertFalse(rs.next());
    }

    @Test
    public void testDelete() throws Exception {
        createTable("testDelete", "`id` INT NOT NULL,"
                + "`id2` INT NOT NULL,"
                + "`t1` VARCHAR(50),"
                + "PRIMARY KEY (`id`,`id2`)");

        Statement stmt = sharedConnection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        stmt.execute("INSERT INTO testDelete values (1,-1,'1'), (2,-2,'2'), (3,-3,'3')");

        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT * FROM testDelete",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();
            try {
                rs.deleteRow();
                fail();
            } catch (SQLException sqle) {
                assertTrue(sqle.getMessage(), sqle.getMessage().contains("Current position is before the first row"));
            }

            assertTrue(rs.next());
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            rs.deleteRow();
            assertEquals(1, rs.getInt(1));
            assertEquals(-1, rs.getInt(2));
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertEquals(-3, rs.getInt(2));
        }


        ResultSet rs = stmt.executeQuery("SELECT * FROM testDelete");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(-1, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(-3, rs.getInt(2));
        assertFalse(rs.next());

        rs.absolute(1);
        rs.deleteRow();
        try {
            rs.getInt(1);
            fail();
        } catch (SQLException sqle) {
            assertTrue(sqle.getMessage(), sqle.getMessage().contains("Current position is before the first row"));
        }

    }

    @Test
    public void testUpdateChangingMultiplePrimaryKey() throws Exception {
        createTable("testUpdateChangingMultiplePrimaryKey", "`id` INT NOT NULL,"
                + "`id2` INT NOT NULL,"
                + "`t1` VARCHAR(50),"
                + "PRIMARY KEY (`id`,`id2`)");

        Statement stmt = sharedConnection.createStatement();
        stmt.execute("INSERT INTO testUpdateChangingMultiplePrimaryKey values (1,-1,'1'), (2,-2,'2'), (3,-3,'3')");
        try (PreparedStatement preparedStatement = sharedConnection.prepareStatement("SELECT * FROM testUpdateChangingMultiplePrimaryKey",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            ResultSet rs = preparedStatement.executeQuery();

            assertTrue(rs.next());
            assertTrue(rs.next());
            rs.updateInt(1, 4);
            rs.updateInt(2, -4);
            rs.updateString(3, "4");
            rs.updateRow();

            assertEquals(4, rs.getInt(1));
            assertEquals(-4, rs.getInt(2));
            assertEquals("4", rs.getString(3));
        }

        ResultSet rs = stmt.executeQuery("SELECT * FROM testUpdateChangingMultiplePrimaryKey");

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(-1, rs.getInt(2));
        assertEquals("1", rs.getString(3));

        assertTrue(rs.next());
        assertEquals(3, rs.getInt(1));
        assertEquals(-3, rs.getInt(2));
        assertEquals("3", rs.getString(3));

        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(-4, rs.getInt(2));
        assertEquals("4", rs.getString(3));

        assertFalse(rs.next());
    }
}
