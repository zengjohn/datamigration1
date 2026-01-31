package com.example.moveprog.repository;

import com.example.moveprog.entity.MigrationJob;
import com.example.moveprog.enums.DetailStatus;
import com.example.moveprog.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * 迁移作业
 */
@Repository
public interface MigrationJobRepository extends JpaRepository<MigrationJob, Long> {

    long countByStatus(JobStatus status);

}