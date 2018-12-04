package com.adbhut.demo.file.batch;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "people")
@Data
@NoArgsConstructor
public class PersonEnitity {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name = "first_name", nullable = false)
    private String lastName;

    @Column(name = "last_name", nullable = false)
    private String firstName;

    @Column(name = "accepted")
    private String accepted = "N";

}
