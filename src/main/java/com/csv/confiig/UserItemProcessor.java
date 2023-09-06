package com.csv.confiig;

import com.csv.model.User;
import org.springframework.batch.item.ItemProcessor;

public class UserItemProcessor implements ItemProcessor<User,User>{
    @Override
    public User process(User item) throws Exception {
        return item;
    }
}
