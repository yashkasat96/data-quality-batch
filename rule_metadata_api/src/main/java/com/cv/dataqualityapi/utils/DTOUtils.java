package com.cv.dataqualityapi.utils;

import java.beans.PropertyDescriptor;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.util.StringUtils;

public class DTOUtils {
	public static void copyPropertiesIgnoringNull(Object source, Object target) {
        BeanUtils.copyProperties(source, target, getNullPropertyNames(source));
    }

    private static String[] getNullPropertyNames(Object source) {
        final BeanWrapper srcWrapper = new BeanWrapperImpl(source);
        PropertyDescriptor[] descriptors = srcWrapper.getPropertyDescriptors();

        Set<String> nullProperties = Arrays.stream(descriptors)
                .filter(descriptor -> srcWrapper.getPropertyValue(descriptor.getName()) == null)
                .map(PropertyDescriptor::getName)
                .collect(Collectors.toSet());

        return StringUtils.toStringArray(nullProperties);
    }
}
