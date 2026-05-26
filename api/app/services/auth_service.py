from passlib.context import CryptContext
from sqlalchemy.orm import Session
from app.models.user import User
from app.auth.jwt import create_access_token

pwd_context = CryptContext(schemes=["sha256_crypt"], deprecated="auto")


def register_user(email: str, password: str, db: Session) -> User:
    if db.query(User).filter(User.email == email).first():
        raise ValueError("Email already registered")
    user = User(email=email, hashed_password=pwd_context.hash(password))
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def login_user(email: str, password: str, db: Session) -> str:
    user = db.query(User).filter(User.email == email).first()
    if not user or not pwd_context.verify(password, user.hashed_password):
        raise ValueError("Invalid credentials")
    return create_access_token({"sub": user.email, "role": user.role})
