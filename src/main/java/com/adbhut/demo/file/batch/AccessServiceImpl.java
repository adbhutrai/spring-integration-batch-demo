package com.adbhut.demo.file.batch;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccessServiceImpl implements AccessService {

    @Override
    public PersonEnitity process(PersonEnitity person) {
        log.info("before uptdate [{}]", person);
        person.setAccepted("Y");
        log.info("after uptdate [{}]", person);
        return person;
    }

}
