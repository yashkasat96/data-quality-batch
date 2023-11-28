package com.cv.dataqualityapi.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_ACCEPTABLE)
public class BusinessException extends RuntimeException {
   
	private static final long serialVersionUID = -6921895758874705639L;

    public BusinessException(String message) {
        super(message);
    }
    
    public BusinessException(String message, Throwable err) {
        super(message, err);
    }

}
