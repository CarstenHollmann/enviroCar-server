/**
 * Copyright (C) 2013  Christian Autermann, Jan Alexander Wirwahn,
 *                     Arne De Wall, Dustin Demuth, Saqib Rasheed
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.car.server.core;


import com.google.inject.Inject;
import com.google.inject.Singleton;

import io.car.server.core.db.GroupDao;
import io.car.server.core.db.UserDao;
import io.car.server.core.exception.GroupNotFoundException;
import io.car.server.core.exception.IllegalModificationException;
import io.car.server.core.exception.ResourceAlreadyExistException;
import io.car.server.core.exception.UserNotFoundException;
import io.car.server.core.exception.ValidationException;

/**
 * @author Christian Autermann <c.autermann@52north.org>
 */
@Singleton
public class UserService {
    private final UserDao userDao;
    private final GroupDao groupDao;
    private final EntityUpdater<User> userUpdater;
    private final EntityValidator<User> userValidator;
    private final EntityUpdater<Group> groupUpdater;
    private final EntityValidator<Group> groupValidator;
    private final PasswordEncoder passwordEncoder;
    @Inject
    public UserService(UserDao userDao, GroupDao groupDao,
                       PasswordEncoder passwordEncoder,
                       EntityUpdater<User> userUpdater,
                       EntityValidator<User> userValidator,
                       EntityUpdater<Group> groupUpdater,
                       EntityValidator<Group> groupValidator) {
        this.userDao = userDao;
        this.groupDao = groupDao;
        this.passwordEncoder = passwordEncoder;
        this.userUpdater = userUpdater;
        this.userValidator = userValidator;
        this.groupUpdater = groupUpdater;
        this.groupValidator = groupValidator;
    }

    public User createUser(User user) throws ValidationException, ResourceAlreadyExistException {
        userValidator.validateCreate(user);
        if (userDao.getByName(user.getName()) != null) {
            throw new ResourceAlreadyExistException();
        }
        user.setToken(passwordEncoder.encode(user.getToken()));
        return this.userDao.create(user);
    }

    public User getUser(String name) throws UserNotFoundException {
        User user = this.userDao.getByName(name);
        if (user == null) {
            throw new UserNotFoundException(name);
        }
        return user;
    }

    public Users getAllUsers(int limit) {
        return this.userDao.getAll(limit);
    }

    public Users getAllUsers() {
        return getAllUsers(0);
    }

    public User modifyUser(User user, User changes) throws UserNotFoundException, IllegalModificationException,
                                                           ValidationException {
        userValidator.validateUpdate(user);
        return this.userDao.save(this.userUpdater.update(changes, user));
    }

    public void deleteUser(String username) throws UserNotFoundException {
        deleteUser(getUser(username));
    }

    public void deleteUser(User user) throws UserNotFoundException {
        this.userDao.delete(user);
    }

    public void removeFriend(User user, User friend) throws UserNotFoundException {
        this.userDao.save(user.removeFriend(getUser(friend.getName())));
    }

    public void addFriend(User user, User friend) throws UserNotFoundException {
        this.userDao.save(user.addFriend(getUser(friend.getName())));
    }

    public Group getGroup(String name) throws GroupNotFoundException {
        Group group = this.groupDao.getByName(name);
        if (group == null) {
            throw new GroupNotFoundException(name);
        }
        return group;
    }

    public Groups getAllGroups(int limit) {
        return this.groupDao.getAll(limit);
    }

    public Group createGroup(Group group) throws ValidationException, ResourceAlreadyExistException {
        groupValidator.validateCreate(group);
        if (groupDao.getByName(group.getName()) != null) {
            throw new ResourceAlreadyExistException();
        }
        return this.groupDao.create(group);
    }

    public Group modifyGroup(Group group, Group changes) throws ValidationException, IllegalModificationException {
        groupValidator.validateUpdate(group);
        return this.groupDao.save(this.groupUpdater.update(changes, group));
    }

    public void deleteGroup(String username) throws GroupNotFoundException {
        deleteGroup(getGroup(username));
    }

    public void deleteGroup(Group user) throws GroupNotFoundException {
        this.groupDao.delete(user);
    }

    public Groups getGroupsOfUser(User user, int limit) {
        return this.groupDao.getByMember(user);
    }

    public Groups searchGroups(String search, int limit) {
        return this.groupDao.search(search, limit);
    }

    public void addGroupMember(Group group, User user) throws UserNotFoundException {
        this.groupDao.save(group.addMember(getUser(user.getName())));
    }

    public void removeGroupMember(Group group, User user) throws UserNotFoundException {
        this.groupDao.save(group.removeMember(getUser(user.getName())));
    }
}