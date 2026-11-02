package org.springframework.samples.petclinic.vet.repository;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.samples.petclinic.vet.model.Vet;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface VetRepository extends JpaRepository<Vet, Integer> {

    @Override
    @EntityGraph(attributePaths = "specialties")
    List<Vet> findAll();
}
