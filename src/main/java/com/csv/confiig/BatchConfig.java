package com.csv.confiig;

import com.csv.model.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {
    @Autowired
    private DataSource dataSource;
    @Autowired
    private JobBuilder jobBuilder;
    @Autowired
    private StepBuilder stepBuilder;


    @Bean
    public FlatFileItemReader<User> reader() {
        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new ClassPathResource("user_data.csv"));
        flatFileItemReader.setLineMapper(getLineMapper());
        flatFileItemReader.setLinesToSkip(1);
        return flatFileItemReader;
    }
    @Bean
    public LineMapper<User> getLineMapper() {
        DefaultLineMapper<User> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setNames("User id", "Name", "Email", "Phone Number", "Address");
//        lineTokenizer.setIncludedFields(new int[]);


        BeanWrapperFieldSetMapper<User> filedSetter = new BeanWrapperFieldSetMapper<>();
        filedSetter.setTargetType(User.class);
        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(filedSetter);
        return lineMapper;
    }

    @Bean
    public UserItemProcessor processor() {
        return new UserItemProcessor();
    }

    @Bean
    public JdbcBatchItemWriter<User> writer() {
        JdbcBatchItemWriter<User> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        writer.setSql("insert into user(id,name.email,phone,adders) values (:id,:name,:email,:phone,:adders");
        writer.setDataSource(dataSource);
        return writer;
    }

    @Bean
    public Job importUserJob() {
        return this.jobBuilder
                .incrementer(new RunIdIncrementer())
                .flow(step1()).
                end()
                .build();
    }

    @Bean
    public Step step1() {
        return this.stepBuilder
                .<User, User>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();

    }
}
