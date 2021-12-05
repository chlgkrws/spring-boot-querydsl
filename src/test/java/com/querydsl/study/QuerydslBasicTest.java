package com.querydsl.study;

import com.querydsl.core.NonUniqueResultException;
import com.querydsl.core.QueryResults;
import com.querydsl.jpa.impl.JPAQueryFactory;
import com.querydsl.study.entity.Member;
import com.querydsl.study.entity.QMember;
import com.querydsl.study.entity.Team;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import java.util.List;

import static com.querydsl.study.entity.QMember.*;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @PersistenceContext
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);
        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

    }

    @Test
    public void startJPQL() {

        //member 1 조회
        String qlString =
                "select m from Member m " +
                "where m.username = :username";

        Member singleResult = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(singleResult.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl() {

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    public void searchForAndOrPriority() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10))
                                .or(member.age.eq(10)))
                        .fetchOne();

        Member findMember2 = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10).or(member.age.eq(10)))
                        .or(member.age.eq(10)))
                .fetchOne();



        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    public void searchAndParam() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1")
                        ,member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    public void resultFetch() {
        //List
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetch();

        assertThat(fetch instanceof List).isTrue();

        //단 건
        Assertions.assertThrows(NonUniqueResultException.class,
                () -> queryFactory
                    .selectFrom(QMember.member)
                    .fetchOne()
        );

        // limit(1) 단 건 조회
        Member firstMember = queryFactory
                .selectFrom(QMember.member)
                .fetchFirst();
        assertThat(firstMember).isNotNull();


        //페이징에서 사용
        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();

        //토탈
        System.out.println("results.getTotal() = " + results.getTotal());
        //컨탠츠
        System.out.println("results.getResults() = " + results.getResults());



        //카운트 쿼리로 변경
        long count = queryFactory
                .selectFrom(member)
                .fetchCount();

        assertThat(count).isGreaterThanOrEqualTo(0);
    }


    /**
     * 멤버 정렬 순서
     * 1. 멤버 나이 내림차순
     * 2. 멤버 이름 올림차순
     * 3. 2에서 멤버 이름이 없으면 마지막에 출력 nulls last
     */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> fetch = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();


        Member member1 = fetch.get(0);
        Member member2 = fetch.get(fetch.size() - 1);
        assertThat(member1.getUsername()).isEqualTo("member5");
        assertThat(member2.getUsername()).isNull();
    }
}



