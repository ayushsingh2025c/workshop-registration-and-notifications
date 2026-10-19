from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import Base, engine, get_db
from app.models import Member, NotificationJob, Registration, Workshop, utcnow
from app.schemas import (
    JobRead,
    MemberCreate,
    MemberRead,
    RegistrationCreate,
    RegistrationRead,
    WorkshopCreate,
    WorkshopRead,
)

@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="Workshop Registration API", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/members", response_model=MemberRead, status_code=status.HTTP_201_CREATED)
def create_member(body: MemberCreate, db: Session = Depends(get_db)):
    member = Member(email=body.email, name=body.name)
    db.add(member)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="A member with this email already exists") from exc
    db.refresh(member)
    return member


@app.post("/workshops", response_model=WorkshopRead, status_code=status.HTTP_201_CREATED)
def create_workshop(body: WorkshopCreate, db: Session = Depends(get_db)):
    workshop = Workshop(title=body.title, description=body.description)
    db.add(workshop)
    db.commit()
    db.refresh(workshop)
    return workshop


@app.get("/workshops/{workshop_id}")
def get_workshop(workshop_id: int, db: Session = Depends(get_db)):
    workshop = db.get(Workshop, workshop_id)
    if workshop is None:
        raise HTTPException(status_code=404, detail="Workshop not found")
    count = db.scalar(
        select(func.count(Registration.id)).where(
            Registration.workshop_id == workshop_id,
            Registration.status == "registered",
        )
    )
    return {**WorkshopRead.model_validate(workshop).model_dump(), "registration_count": count}


@app.post("/registrations", response_model=RegistrationRead, status_code=status.HTTP_201_CREATED)
def register(body: RegistrationCreate, db: Session = Depends(get_db)):
    workshop = db.get(Workshop, body.workshop_id)
    if workshop is None:
        raise HTTPException(status_code=404, detail="Workshop not found")
    if db.get(Member, body.member_id) is None:
        raise HTTPException(status_code=404, detail="Member not found")
    if workshop.status != "active":
        raise HTTPException(status_code=409, detail="Workshop is cancelled")

    registration = Registration(workshop_id=body.workshop_id, member_id=body.member_id)
    db.add(registration)
    try:
        db.flush()
        job = NotificationJob(
            workshop_id=body.workshop_id,
            member_id=body.member_id,
            registration_id=registration.id,
            notification_type="confirmation",
            idempotency_key=f"registration:{registration.id}:confirmation",
        )
        db.add(job)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="Member is already registered for this workshop") from exc
    db.refresh(registration)
    db.refresh(job)
    return RegistrationRead(
        id=registration.id,
        workshop_id=registration.workshop_id,
        member_id=registration.member_id,
        status=registration.status,
        notification_job_id=job.id,
    )


@app.post("/workshops/{workshop_id}/cancel", status_code=status.HTTP_202_ACCEPTED)
def cancel_workshop(workshop_id: int, db: Session = Depends(get_db)):
    workshop = db.get(Workshop, workshop_id)
    if workshop is None:
        raise HTTPException(status_code=404, detail="Workshop not found")
    if workshop.status == "cancelled":
        count = db.scalar(
            select(func.count(NotificationJob.id)).where(
                NotificationJob.workshop_id == workshop_id,
                NotificationJob.notification_type == "cancellation",
            )
        )
        return {"workshop_id": workshop_id, "status": "cancelled", "notification_jobs_created": count}

    registrations = db.scalars(
        select(Registration).where(
            Registration.workshop_id == workshop_id,
            Registration.status == "registered",
        )
    ).all()
    workshop.status = "cancelled"
    for registration in registrations:
        registration.status = "cancelled"
        db.add(
            NotificationJob(
                workshop_id=workshop_id,
                member_id=registration.member_id,
                registration_id=registration.id,
                notification_type="cancellation",
                idempotency_key=f"registration:{registration.id}:cancellation",
            )
        )
    db.commit()
    return {
        "workshop_id": workshop_id,
        "status": "cancelled",
        "notification_jobs_created": len(registrations),
    }


@app.get("/notification-jobs/{job_id}", response_model=JobRead)
def get_notification_job(job_id: int, db: Session = Depends(get_db)):
    job = db.get(NotificationJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Notification job not found")
    return job
